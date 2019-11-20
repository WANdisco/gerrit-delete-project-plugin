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

import com.google.common.flogger.FluentLogger;
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
import com.google.gerrit.server.query.change.ChangeData;
import com.google.gerrit.server.replication.*;
import com.google.gwtorm.server.OrmConcurrencyException;
import com.google.gwtorm.server.OrmException;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.googlesource.gerrit.plugins.deleteproject.DeleteProject.Input;
import com.googlesource.gerrit.plugins.deleteproject.cache.CacheDeleteHandler;
import com.googlesource.gerrit.plugins.deleteproject.database.DatabaseDeleteHandler;
import com.googlesource.gerrit.plugins.deleteproject.fs.FilesystemDeleteHandler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.http.client.utils.URIBuilder;
import org.eclipse.jgit.errors.ConfigInvalidException;
import org.eclipse.jgit.errors.RepositoryNotFoundException;
import org.eclipse.jgit.storage.file.FileBasedConfig;
import org.eclipse.jgit.util.FS;
import org.eclipse.jgit.util.StringUtils;

@Singleton
class DeleteProject implements RestModifyView<ProjectResource, Input> {
  private static final FluentLogger log = FluentLogger.forEnclosingClass();
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

  public void doDelete(ProjectResource rsrc, Input input) throws OrmException, IOException, RestApiException {
    Project project = rsrc.getProjectState().getProject();
    boolean preserve = input != null && input.preserve;

    // Check if the repo is replicated
    boolean replicatedRepo = isRepoReplicated(project);

    if (!replicatedRepo){
      deleteNonReplicatedRepo(rsrc, input);
    }else{
      deleteReplicatedRepo(rsrc, input, project, preserve);
    }
  }


  public void deleteNonReplicatedRepo(ProjectResource rsrc, Input input)
      throws OrmException, IOException, RestApiException {
    Project project = rsrc.getProjectState().getProject();
    boolean preserve = input != null && input.preserve;
    Exception ex = null;
    try {
      if (!preserve || !cfg.projectOnPreserveHidden()) {
        if (!migration.disableChangeReviewDb()) {
          dbHandler.delete(project);
        }
        try {
          fsHandler.delete(project, preserve);
        } catch (RepositoryNotFoundException e) {
          throw new ResourceNotFoundException();
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

  private final static String checkIfReplicated(final String repoParentPath, final String projectName) throws IOException {
    String replicatedProperty = null;
    String repoLocation = repoParentPath + "/" + projectName;

    String repoConfig = repoLocation + "/" + "config";

    if (!StringUtils.isEmptyOrNull(repoConfig)) {
      File repoConfigFile = new File(repoConfig);
      if (repoConfigFile.canRead()) {
        replicatedProperty = getProperty(repoConfigFile, "replicated");
      }
    }

    return replicatedProperty;
  }

  public boolean isRepoReplicated(Project project) throws IOException {
    log.atFine().log("Verifying if project: {} is replicated.", project.getName());
    boolean replicatedRepo = isRepoReplicated(project.getName());
    log.atFine().log("Replicated property: {}", replicatedRepo);
    return replicatedRepo;
  }


  public static boolean isRepoReplicated(String projectName) throws IOException {
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
      replicatedProperty = checkIfReplicated(repoParentPath, projectName + ".git");

      // the project may not have a .git extension so try without it
      if (replicatedProperty == null) {
        replicatedProperty = checkIfReplicated(repoParentPath, projectName);
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

  private HttpURLConnection gitmsDeleteRequest(String repoPath, String projectName, String port, String taskIdForDelayedRemoval)
          throws IOException, URISyntaxException {

    repoPath = URLEncoder.encode(repoPath + "/" + projectName, "UTF-8");
    String gitmsDeleteEndpoint = String.format("/gerrit/delete?repoPath=%s%s", repoPath, taskIdForDelayedRemoval);

    URI builder = new URIBuilder().setScheme("http").setHost("127.0.0.1").setPort(Integer.parseInt(port)).setPath(gitmsDeleteEndpoint).build();
    URL url = builder.toURL();

    log.atInfo().log("Calling URL {}...", url);
    HttpURLConnection httpCon = (HttpURLConnection) url.openConnection();
    httpCon.setDoOutput(true);
    httpCon.setUseCaches(false);
    httpCon.setRequestMethod("DELETE");
    httpCon.setRequestProperty("Content-Type", "application/xml");
    httpCon.setRequestProperty("Accept", "application/xml");
    return httpCon;
  }

  public void archiveAndRemoveRepo(Project project, String uuid) throws IOException {
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

    // get projectName, assume .git as default. If it doesn't exist, try without .git
    File projectPath = new File(repoPath + "/" + project.getName() + ".git");
    String projectName = project.getName() + ".git";
    if (!projectPath.exists()) {
      projectPath = new File(repoPath + "/" + project.getName());
      if (projectPath.exists()) {
        projectName = project.getName();
      }
    }

    if (port != null && !port.isEmpty()) {
      String taskIdForDelayedRemoval = "&taskIdForDelayedRemoval="+uuid;
      try {
        HttpURLConnection httpURLConnection = gitmsDeleteRequest(repoPath, projectName, port, taskIdForDelayedRemoval);

        //an error may have happened, and if it did, the errorstream will be available
        //to get more details - but if repo deletion was successful, getErrorStream
        //will be null
        StringBuilder responseString = new StringBuilder();
        if (httpURLConnection.getErrorStream() != null) {
          try (BufferedReader reader = new BufferedReader(new InputStreamReader(httpURLConnection.getErrorStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
              responseString.append(line).append("\n");
            }
          }
        }
        httpURLConnection.disconnect();

        int responseCode = httpURLConnection.getResponseCode();
        if (responseCode != 202 && responseCode != 200) {
          //there has been a problem with the deletion
          throw new IOException(String.format("Failure to delete the git repository on the GitMS Replicator, " +
                  "response code: %s, Replicator response was: %s", responseCode, responseString.toString()));
        }
      } catch (IOException | URISyntaxException e) {
        IOException ee = new IOException("Error with deleting repo: " + e.toString());
        ee.initCause(e);
        log.atSevere().log("Error with deleting repo: {}", repoPath, ee);
        throw ee;
      }
    }
  }


  /*
   * This new method is used to clean up Replicated projects.
   * There are 2 paths that can be taken..
   * 1. The user only wants to remove the projects metadata, but leave the project replicating in Gerrit and GitMS
   * 2. The user wants to completely remove the project from Gerrit, GitMS and on disk
   */
  public void deleteReplicatedRepo(ProjectResource rsrc, Input input, Project project, boolean preserve)
          throws OrmException, IOException, RestApiException {

    Exception ex = null;
    log.atFine().log("Deleting the replicated project: {}", project.getName());

    String uuid = UUID.randomUUID().toString();
    List<ChangeData> changes = new ArrayList<ChangeData>();

    try {
      if (!preserve || !cfg.projectOnPreserveHidden()) {
        log.atInfo().log("Preserve flag is set to: {}", preserve);

        // Archive repo before we attempt to delete
        // The Repo only needs to be archived and removed from GITMS, if the "preserve" flag is not selected.
        if(!preserve){
          archiveAndRemoveRepo(project, uuid);
        }

        try {
          changes = dbHandler.replicatedDeleteChanges(project);
          log.atInfo().log("Deletion of project {} from the database succeeded", project.getName());
        } catch(OrmConcurrencyException e) {
          log.atSevere().log("Could not delete the project {}", project.getName(), e);
          throw e;
        }

        try {
          // We no longer remove the repo on disk here, we let GITMS handle this
          // But we still need to remove from the jgit cache
          fsHandler.deleteFromCache(project);
        } catch (RepositoryNotFoundException e) {
          log.atSevere().log("Could not find the project {}", project.getName(), e);
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
      log.atInfo().log("About to call ReplicatedProjectManager.replicateProjectDeletion(): {}, {}", project.getName(), preserve);
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
    log.atInfo().log("About to call ReplicatedProjectManager.replicateProjectChangeDeletion(): {}, {}", project.getName(), preserve);
    // Get the Change.Id from the ChangeData
    List<Change.Id> changeIds = new ArrayList<>();
    for (ChangeData cd: changes) {
      changeIds.add(cd.getId());
    }

    // Delete changes locally
    ReplicatedIndexEventManager.getInstance().deleteChanges(changeIds);

    //Replicate changesToBeDeleted across the nodes
    String uuid = UUID.randomUUID().toString();
    ReplicatedProjectManager.replicateProjectChangeDeletion(project, preserve, changeIds, uuid);
  }
}
