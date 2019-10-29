// Copyright (C) 2013 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.googlesource.gerrit.plugins.deleteproject;

import static com.googlesource.gerrit.plugins.deleteproject.DeleteOwnProjectCapability.DELETE_OWN_PROJECT;
import static com.googlesource.gerrit.plugins.deleteproject.DeleteProjectCapability.DELETE_PROJECT;

import com.google.gerrit.common.ReplicatedIndexEventManager;
import com.google.gerrit.common.ReplicatedProjectManager;
import com.google.gerrit.extensions.annotations.PluginName;
import com.google.gerrit.extensions.restapi.AuthException;
import com.google.gerrit.extensions.restapi.ResourceConflictException;
import com.google.gerrit.extensions.restapi.ResourceNotFoundException;
import com.google.gerrit.extensions.restapi.Response;
import com.google.gerrit.extensions.restapi.RestApiException;
import com.google.gerrit.extensions.restapi.RestModifyView;
import com.google.gerrit.reviewdb.client.Change;
import com.google.gerrit.reviewdb.client.Project;
import com.google.gerrit.server.CurrentUser;
import com.google.gerrit.server.IdentifiedUser;
import com.google.gerrit.server.notedb.NotesMigration;
import com.google.gerrit.server.project.ProjectResource;
import com.google.gwtorm.server.OrmConcurrencyException;
import com.google.gwtorm.server.OrmException;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.googlesource.gerrit.plugins.deleteproject.DeleteProject.Input;
import com.googlesource.gerrit.plugins.deleteproject.cache.CacheDeleteHandler;
import com.googlesource.gerrit.plugins.deleteproject.database.DatabaseDeleteHandler;
import com.googlesource.gerrit.plugins.deleteproject.fs.FilesystemDeleteHandler;
import java.io.IOException;
import org.eclipse.jgit.errors.RepositoryNotFoundException;
import org.eclipse.jgit.storage.file.FileBasedConfig;
import org.eclipse.jgit.util.FS;
import org.eclipse.jgit.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
class DeleteProject implements RestModifyView<ProjectResource, Input> {
  static class Input {
    boolean preserve;
    boolean force;
  }

  protected final DeletePreconditions preConditions;

  private final DatabaseDeleteHandler dbHandler;
  private final FilesystemDeleteHandler fsHandler;
  private final CacheDeleteHandler cacheHandler;
  private final Provider<CurrentUser> userProvider;
  private final DeleteLog deleteLog;
  private final Configuration cfg;
  private final HideProject hideProject;
  private NotesMigration migration;

  final Logger log = LoggerFactory.getLogger(DeleteProject.class);

  @Inject
  DeleteProject(
      DatabaseDeleteHandler dbHandler,
      FilesystemDeleteHandler fsHandler,
      CacheDeleteHandler cacheHandler,
      Provider<CurrentUser> userProvider,
      DeleteLog deleteLog,
      DeletePreconditions preConditions,
      Configuration cfg,
      HideProject hideProject,
      NotesMigration migration) {
    this.dbHandler = dbHandler;
    this.fsHandler = fsHandler;
    this.cacheHandler = cacheHandler;
    this.userProvider = userProvider;
    this.deleteLog = deleteLog;
    this.preConditions = preConditions;
    this.cfg = cfg;
    this.hideProject = hideProject;
    this.migration = migration;
  }

  @Override
  public Object apply(ProjectResource rsrc, Input input)
      throws OrmException, IOException, RestApiException {
    preConditions.assertDeletePermission(rsrc);
    preConditions.assertCanBeDeleted(rsrc, input);

    doDelete(rsrc, input);
    return Response.none();
  }

  public void assertDeletePermission(ProjectResource rsrc)
      throws AuthException {
    if (!canDelete(rsrc)) {
      throw new AuthException("not allowed to delete project");
    }
  }

  protected boolean canDelete(ProjectResource rsrc) {
    CapabilityControl ctl = userProvider.get().getCapabilities();
    return ctl.canAdministrateServer()
        || ctl.canPerform(pluginName + "-" + DELETE_PROJECT)
        || (ctl.canPerform(pluginName + "-" + DELETE_OWN_PROJECT)
            && rsrc.getControl().isOwner());
  }

  public void assertCanDelete(ProjectResource rsrc, Input input)
      throws ResourceConflictException {
    try {
      pcHandler.assertCanDelete(rsrc);
      dbHandler.assertCanDelete(rsrc.getControl().getProject());
      fsHandler.assertCanDelete(rsrc, input == null ? false : input.preserve);
    } catch (CannotDeleteProjectException e) {
      throw new ResourceConflictException(e.getMessage());
    }
  }

  public Collection<String> getWarnings(ProjectResource rsrc)
      throws OrmException {
    return dbHandler.getWarnings(rsrc.getControl().getProject());
  }

  public void doDelete(ProjectResource rsrc, Input input)
      throws OrmException, IOException, RestApiException {
    Project project = rsrc.getProjectState().getProject();
    boolean preserve = input != null && input.preserve;

    // Check if the repo is replicated
    boolean replicatedRepo = isRepoReplicated(project);

    if (!replicatedRepo){
      deleteNonReplicatedRepo(rsrc, input, project, preserve);
    }else{
      deleteReplicatedRepo(rsrc, input, project, preserve);
    }
  }

  public void archiveAndRemoveRepo(Project project, String uuid) throws IOException {
    Logger log = LoggerFactory.getLogger(ReplicatedProjectManager.class);
    // get the GitMS config file
    FileBasedConfig config = getConfigFile();

    String port = null;
    String repoPath = null;
    String appProperties = config.getString("core", null, "gitmsconfig");

    if (!StringUtils.isEmptyOrNull(appProperties)) {
      File appPropertiesFile = new File(appProperties);
      if (appPropertiesFile.canRead()) {
        port = getProperty(appPropertiesFile, "gitms.local.jetty.port");
        repoPath = getProperty(appPropertiesFile, "gerrit.repo.home");
      }
    }

    if (port != null && !port.isEmpty()) {
      String taskIdForDelayedRemoval = "&taskIdForDelayedRemoval="+uuid;
      try {
        repoPath = URLEncoder.encode(repoPath + "/" + project.getName() + ".git", "UTF-8");
        URL url = new URL("http://127.0.0.1:" + port + "/gerrit/delete?" + "repoPath=" + repoPath + taskIdForDelayedRemoval);
        log.info("Calling URL {}...", url);
        HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
        httpCon.setDoOutput(true);
        httpCon.setUseCaches(false);
        httpCon.setRequestMethod("DELETE");
        httpCon.setRequestProperty("Content-Type", "application/xml");
        httpCon.setRequestProperty("Accept", "application/xml");
        int response = httpCon.getResponseCode();

        //an error may have happened, and if it did, the errorstream will be available
        //to get more details - but if repo deletion was successful, getErrorStream
        //will be null
        StringBuilder responseString = new StringBuilder();
        if (httpCon.getErrorStream() != null) {
          try (BufferedReader reader = new BufferedReader(new InputStreamReader(httpCon.getErrorStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
              responseString.append(line).append("\n");
            }
          }
        }
        httpCon.disconnect();

        if (response != 202 && response != 200) {
          //there has been a problem with the deletion
          throw new IOException("Failure to delete the git repository on the GitMS Replicator, response code: " + response
              + " Replicator response: " + responseString.toString());
        }
      } catch (IOException e) {
        IOException ee = new IOException("Error with deleting repo: " + e.toString());
        ee.initCause(e);
        log.error("Error with deleting repo: {}", repoPath, ee);
        throw ee;
      }
    }
  }

  public static String getProperty(File appProps, String propertyName) throws IOException {
    Properties props = new Properties();
    InputStream input = null;
    try {
      input = new FileInputStream(appProps);
      props.load(input);
      return props.getProperty(propertyName);
    } catch (IOException e) {
      throw new IOException("Could not read " + appProps.getAbsolutePath());
    } finally {
      if (input != null) {
        try {
          input.close();
        } catch (IOException ex) {
          //noop
        }
      }
    }
  }

  public static FileBasedConfig getConfigFile() throws IOException {
    String gitConfigLoc = System.getenv("GIT_CONFIG");

    if (System.getenv("GIT_CONFIG") == null) {
      gitConfigLoc = System.getProperty("user.home") + "/.gitconfig";
    }

    FileBasedConfig config = new FileBasedConfig(new File(gitConfigLoc), FS.DETECTED);
    try {
      config.load();
    } catch (ConfigInvalidException e) {
      // Configuration file is not in the valid format, throw exception back.
      throw new IOException(e);
    }
    return config;
  }

  /**
   * This static method is used by the DeleteAction class to allow us to change the text that displays
   * on the pop-up when we are removing a project.
   * @param project
   * @return
   * @throws IOException
   */
  public static boolean isRepoReplicated(ProjectResource project) throws IOException {
    boolean replicatedRepo = false;

    // get the GitMS config file
    FileBasedConfig config = getConfigFile();

    String repoParentPath = null;
    String replicatedProperty = null;
    String appProperties = config.getString("core", null, "gitmsconfig");

    if (!StringUtils.isEmptyOrNull(appProperties)) {
      File appPropertiesFile = new File(appProperties);
      if (appPropertiesFile.canRead()) {
        repoParentPath = getProperty(appPropertiesFile, "gerrit.repo.home");
      }
    }

    if (repoParentPath != null) {
      String repoLocation = repoParentPath + "/" + project.getName() + ".git";

      String repoConfig = repoLocation + "/" + "config";

      if (!StringUtils.isEmptyOrNull(repoConfig)) {
        File repoConfigFile = new File(repoConfig);
        if (repoConfigFile.canRead()) {
          replicatedProperty = getProperty(repoConfigFile, "replicated");
        }
      }
    }

    if (replicatedProperty == null || replicatedProperty.equalsIgnoreCase("false")) {
      replicatedRepo = false;
    } else if (replicatedProperty.equalsIgnoreCase("true")) {
      replicatedRepo = true;
    } else {
      replicatedRepo = false;
    }

    return replicatedRepo;
  }

  public boolean isRepoReplicated(Project project) throws IOException {
    final Logger log = LoggerFactory.getLogger(DeleteProject.class);
    log.debug("Verifying if project: " + project.getName() + " is replicated.");

    boolean replicatedRepo = false;

    // get the GitMS config file
    FileBasedConfig config = getConfigFile();

    String repoParentPath = null;
    String replicatedProperty = null;
    String appProperties = config.getString("core", null, "gitmsconfig");

    if (!StringUtils.isEmptyOrNull(appProperties)) {
      File appPropertiesFile = new File(appProperties);
      if (appPropertiesFile.canRead()) {
        repoParentPath = getProperty(appPropertiesFile, "gerrit.repo.home");
      }
    }

    if (repoParentPath != null) {
      String repoLocation = repoParentPath + "/" + project.getName() + ".git";
      log.debug("Repo Location: " + repoLocation);

      String repoConfig = repoLocation + "/" + "config";

      if (!StringUtils.isEmptyOrNull(repoConfig)) {
        File repoConfigFile = new File(repoConfig);
        if (repoConfigFile.canRead()) {
          replicatedProperty = getProperty(repoConfigFile, "replicated");
        }
      }
    }

    if (replicatedProperty == null || replicatedProperty.equalsIgnoreCase("false")) {
      replicatedRepo = false;
    } else if (replicatedProperty.equalsIgnoreCase("true")) {
      replicatedRepo = true;
    } else {
      replicatedRepo = false;
    }

    log.debug("Replicated property: " + replicatedProperty);

    return replicatedRepo;
  }

  /*
   * This method specifically deals with Non-Replicated projects and follows the original process of deleteing projects
   *
   */
  public void deleteNonReplicatedRepo(ProjectResource rsrc, Input input, Project project, boolean preserve)
      throws OrmException, IOException, ResourceNotFoundException, ResourceConflictException {
    final Logger log = LoggerFactory.getLogger(DeleteProject.class);
    Exception ex = null;
    log.debug("Deleting the non-replicated project: " + project.getName());
    try {
      if (!preserve || !cfg.projectOnPreserveHidden()) {
        if (!migration.disableChangeReviewDb()) {
          dbHandler.delete(project);
        }
        try {
          fsHandler.delete(project, preserve);
        } catch (RepositoryNotFoundException e) {
          throw e;
        }
        cacheHandler.delete(project);
      } else {
        hideProject.apply(rsrc);
      }
    } catch (Exception e) {
      ex = e;
      throw e;
    } finally {
      deleteLog.onDelete((IdentifiedUser) userProvider.get(), project.getNameKey(), input, ex);
    }
  }

  /*
   * This new method is used to clean up Replicated projects.
   * There are 2 paths that can be taken..
   * 1. The user only wants to remove the projects metadata, but leave the project replicating in Gerrit and GitMS
   * 2. The user wants to completely remove the project from Gerrit, GitMS and on disk
   */
  public void deleteReplicatedRepo(ProjectResource rsrc, Input input, Project project, boolean preserve)
      throws OrmException, IOException, ResourceNotFoundException, ResourceConflictException {

    Exception ex = null;
    log.debug("Cleaning up the replicated project: " + project.getName());

    String uuid = UUID.randomUUID().toString();
    List<ChangeData> changes = new ArrayList<ChangeData>();

    try {
      if (!preserve || !cfgFactory.getFromGerritConfig(pluginName).getBoolean("hideProjectOnPreserve", false)) {
        log.info("Preserve flag is set to: {}", preserve);

        // Archive repo before we attempt to delete
        // The Repo only needs to be archived and removed from GITMS, if the "preserve" flag is not selected.
        if(!preserve){
          archiveAndRemoveRepo(project, uuid);
        }

        try {
          changes = dbHandler.replicatedDeleteChanges(project);
          log.info("Deletion of project {} from the database succeeded", project.getName());
        } catch(OrmConcurrencyException e) {
          log.error("Could not delete the project {}", project.getName(), e);
          throw e;
        }

        try {
          // We no longer remove the repo on disk here, we let GITMS handle this
          // But we still need to remove from the jgit cache
          fsHandler.deleteFromCache(project);
        } catch (RepositoryNotFoundException e) {
          log.error("Could not find the project {}", project.getName(), e);
          throw e;
        }

        // We also only remove it from the project list and local cache if the "preserve" flag is not selected.
        if(!preserve){
          cacheHandler.delete(project);
        }
      } else {
        hideProject.apply(rsrc);
      }

      // Replicate the deletion of project changes
      if (changes != null){
        deleteProjectChanges(preserve, changes, project);
      }

      // Replicate the project deletion (NOTE this is the project deletion within Gerrit, from the project cache)
      log.info("About to call ReplicatedProjectManager.replicateProjectDeletion(): {}, {}", project.getName(), preserve);
      ReplicatedProjectManager.replicateProjectDeletion(project.getName(), preserve, uuid);

    } catch (Exception e) {
      ex = e;
      throw e;
    } finally {
      deleteLog.onDelete((IdentifiedUser) userProvider.get(), project.getNameKey(), input, ex);
    }
  }


  /**
   * Replicate the call to delete the project changes.
   * @param preserve
   * @param changes
   * @throws IOException
   */
  public void deleteProjectChanges(boolean preserve, List<ChangeData> changes, Project project) throws IOException{
    log.info("About to call ReplicatedProjectManager.replicateProjectChangeDeletion(): {}, {}", project.getName(), preserve);
    // Get the Change.Id from the ChangeData
    List<Change.Id> changeIds = new ArrayList<>();
    for (ChangeData cd: changes) {
      changeIds.add(cd.getId());
    }

    // Delete changes locally
    ReplicatedIndexEventManager.getInstance().deleteChanges(changes);

    //Replicate changesToBeDeleted across the nodes
    String uuid = UUID.randomUUID().toString();
    ReplicatedProjectManager.replicateProjectChangeDeletion(project, preserve, changeIds, uuid);
  }
}
