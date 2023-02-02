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
import com.google.gerrit.server.project.ProjectResource;
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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import com.wandisco.gerrit.gitms.shared.api.ApiResponse;
import com.wandisco.gerrit.gitms.shared.api.HttpRequestBuilder;
import com.wandisco.gerrit.gitms.shared.exception.ConfigurationException;
import com.wandisco.gerrit.gitms.shared.properties.GitMsApplicationProperties;
import org.eclipse.jgit.errors.ConfigInvalidException;
import org.eclipse.jgit.errors.RepositoryNotFoundException;
import org.eclipse.jgit.storage.file.FileBasedConfig;
import org.eclipse.jgit.util.FS;
import org.eclipse.jgit.util.StringUtils;

import static com.wandisco.gerrit.gitms.shared.api.HttpRequestBuilder.WebRequestType.DELETE;

@Singleton
class DeleteProject implements RestModifyView<ProjectResource, Input> {
  private static final FluentLogger log = FluentLogger.forEnclosingClass();
  static class Input {
    boolean preserve;
    boolean force;
  }

  private final HttpRequestBuilder requestBuilder;
  private final String deleteEndpoint = "/gerrit/delete";

  protected final DeletePreconditions preConditions;

  private final DatabaseDeleteHandler dbHandler;
  private final FilesystemDeleteHandler fsHandler;
  private final CacheDeleteHandler cacheHandler;
  private final Provider<CurrentUser> userProvider;
  private final DeleteLog deleteLog;
  private final Configuration cfg;
  private final HideProject hideProject;

  @Inject
  DeleteProject(
      DatabaseDeleteHandler dbHandler,
      FilesystemDeleteHandler fsHandler,
      CacheDeleteHandler cacheHandler,
      Provider<CurrentUser> userProvider,
      DeleteLog deleteLog,
      DeletePreconditions preConditions,
      Configuration cfg,
      HideProject hideProject) {
    this.dbHandler = dbHandler;
    this.fsHandler = fsHandler;
    this.cacheHandler = cacheHandler;
    this.userProvider = userProvider;
    this.deleteLog = deleteLog;
    this.preConditions = preConditions;
    this.cfg = cfg;
    this.hideProject = hideProject;
    this.requestBuilder = Replicator.isReplicationDisabled() ? null : getRequestBuilder();
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
        dbHandler.delete(project);
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
    log.atFine().log("Verifying if project: %s is replicated.", project.getName());
    boolean replicatedRepo = isRepoReplicated(project.getName());
    log.atFine().log("Replicated property: %s", replicatedRepo);
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


  /*
   * Creates a HTTP request object with the desired request type as DELETE.
   */
  private static HttpRequestBuilder setupHttpRequest(final String host, final int port, final String endpoint) {
    HttpRequestBuilder request
        = new HttpRequestBuilder(
            host,
            port,
            endpoint);

    // Update to use DELETE instead of POST which is default.
    request.setRequestType(DELETE);
    return request;
  }


  /*
   * The user wants to completely remove the project from Gerrit, GitMS and on disk
   * The repository will be archived
   */
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
      try {
        log.atInfo().log("Calling URL %s ...",
            URLDecoder.decode(requestBuilder.getBaseRequestURI().build().toURL().toString()));
        makeGitmsDeleteRequest(uuid, projectPath.toString());
      } catch (IOException e) {
        IOException ee = new IOException("Error with deleting repo: " + e.toString());
        ee.initCause(e);
        log.atSevere().log("Error with deleting repo: %s", projectPath.toString(), ee);
        throw ee;
      } catch (URISyntaxException e) {
        log.atSevere().log("Error with deleting repo: %s", projectPath.toString(), e);
      }
    }
  }


  /*
   * Request parameters appended onto the delete request of the form
   * /gerrit/delete?repoPath=/path/to/repo.git&taskIdForDelayedRemoval=1234
   */
  private void setRequestParameters(String repoPath, String uuid) {
    if(StringUtils.isEmptyOrNull(repoPath)){
      throw new InvalidParameterException("Delete-Project DELETE request requires field " +
          "'repoPath' to be specified with a valid path of a repository to be deleted");
    }

    if(StringUtils.isEmptyOrNull(uuid)){
      throw new InvalidParameterException("Delete-Project DELETE request requires field " +
          "'taskIdForDelayedRemoval' to be specified with a UUID");
    }

    requestBuilder.setRequestParameter("repoPath", repoPath);
    requestBuilder.setRequestParameter("taskIdForDelayedRemoval", uuid);
  }

  private HttpRequestBuilder getRequestBuilder() {
    final GitMsApplicationProperties appProps = Replicator.getApplicationProperties();
    return setupHttpRequest(appProps.getGitMSLocalJettyHost(),
                            Integer.parseInt(appProps.getGitMSLocalJettyPort()),
                            this.deleteEndpoint);
  }

  /*
   * Makes the request to GitMS /gerrit/delete endpoint for the
   * repository path to be deleted.
   */
  private void makeGitmsDeleteRequest(String uuid, String repoPath) throws IOException {

    //Set the parameters for the request if they are valid.
    setRequestParameters(repoPath, uuid);

    ApiResponse response = requestBuilder.makeRequest();

    log.atInfo().log("Made the following request [ %s %s ] ",
        requestBuilder.getRequest().getMethod(),
        URLDecoder.decode(requestBuilder.getRequest().getURI().toURL().toString()));


    if (response.statusCode != 202 && response.statusCode != 200) {
      //there has been a problem with the deletion
      throw new IOException(String.format("Failure to delete the git repository on the GitMS Replicator, " +
              "response code: %s, Replicator response was: %s", response, response.response));
    }
  }


  /*
   * This new method is used to clean up Replicated projects.
   * There are 2 paths that can be taken..
   * 1. The user only wants to remove the projects metadata, but leave the project replicating in Gerrit and GitMS
   * 2. The user wants to completely remove the project from Gerrit, GitMS and on disk
   * Consult README.md for further information.
   */
  public void deleteReplicatedRepo(ProjectResource rsrc, Input input, Project project, boolean preserve)
          throws OrmException, IOException, RestApiException {

    Exception ex = null;
    log.atFine().log("Deleting the replicated project: %s", project.getName());

    String uuid = UUID.randomUUID().toString();
    List<Change.Id> changeIds = new ArrayList<>();

    try {
      if (!preserve || !cfg.projectOnPreserveHidden()) {
        log.atInfo().log("Preserve flag is set to: %s", preserve);

        // Archive repo before we attempt to delete
        // The Repo only needs to be archived and removed from GITMS, if the "preserve" flag is not selected.

        //When preserve is false Remove Gerrit project data, remove from replication and archive repository
        //The project and all associated changes, reviews etc will be removed from Gerrit, the repo will be removed
        //from replication in GitMS and will no longer exist on disk.
        //However, before removal a zipped version of the repo will be archived to the directory specified during install.
        if(!preserve){
          archiveAndRemoveRepo(project, uuid);
        }

        try {
          changeIds = dbHandler.getReplicatedDeleteChangeIdsList(project);
          log.atInfo().log("Deletion of project %s from the database succeeded", project.getName());
        } catch(OrmConcurrencyException e) {
          log.atSevere().log("Could not delete the project %s", project.getName(), e);
          throw e;
        }

        try {
          // We no longer remove the repo on disk here, we let GITMS handle this
          // But we still need to remove from the jgit cache
          fsHandler.deleteFromCache(project);
        } catch (RepositoryNotFoundException e) {
          log.atSevere().log("Could not find the project %s", project.getName(), e);
          throw e;
        }

        // We also only remove it from the project list and local cache if the "preserve" flag is not selected, i.e false
        if(!preserve){
          cacheHandler.delete(project);
        }
      } else {
        hideProject.apply(rsrc);
      }

      // Replicate the deletion of project changes
      if (changeIds != null){
        deleteProjectChanges(preserve, changeIds, project);
      }

      // Replicate the project deletion (NOTE this is the project deletion within Gerrit, from the project cache)
      log.atInfo().log("About to call ReplicatedProjectManager.replicateProjectDeletion(): %s, %s", project.getName(), preserve);
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
   * @param changeIds
   * @throws IOException
   */
  public void deleteProjectChanges(boolean preserve, List<Change.Id> changeIds, Project project) throws IOException{
    log.atInfo().log("About to call ReplicatedProjectManager.replicateProjectChangeDeletion(): %s, %s", project.getName(), preserve);

    // Delete changes locally
    ReplicatedIndexEventManager.getInstance().deleteChanges(changeIds);

    //Replicate changesToBeDeleted across the nodes
    String uuid = UUID.randomUUID().toString();
    ReplicatedProjectManager.replicateProjectChangeDeletion(project, preserve, changeIds, uuid);
  }
}
