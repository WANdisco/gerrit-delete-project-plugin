
/********************************************************************************
 * Copyright (c) 2014-2018 WANdisco
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Apache License, Version 2.0
 *
 ********************************************************************************/

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

import com.google.gerrit.common.replication.coordinators.ReplicatedEventsCoordinator;
import com.google.gerrit.extensions.annotations.PluginName;
import com.google.gerrit.extensions.webui.UiAction;
import com.google.gerrit.server.CurrentUser;
import com.google.gerrit.server.config.AllProjectsNameProvider;
import com.google.gerrit.server.config.AllUsersNameProvider;
import com.google.gerrit.server.config.PluginConfigFactory;
import com.google.gerrit.server.project.ProjectResource;
import com.google.inject.Inject;
import com.google.inject.Provider;

import com.googlesource.gerrit.plugins.deleteproject.cache.CacheDeleteHandler;
import com.googlesource.gerrit.plugins.deleteproject.database.DatabaseDeleteHandler;
import com.googlesource.gerrit.plugins.deleteproject.fs.FilesystemDeleteHandler;
import com.googlesource.gerrit.plugins.deleteproject.projectconfig.ProjectConfigDeleteHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DeleteAction extends DeleteProject implements
    UiAction<ProjectResource> {
  @Inject
  DeleteAction(AllProjectsNameProvider allProjectsNameProvider,
               AllUsersNameProvider allUsersNameProvider,
               DatabaseDeleteHandler dbHandler,
               FilesystemDeleteHandler fsHandler,
               CacheDeleteHandler cacheHandler,
               ProjectConfigDeleteHandler pcHandler,
               Provider<CurrentUser> userProvider,
               @PluginName String pluginName,
               DeleteLog deleteLog,
               PluginConfigFactory cfgFactory,
               HideProject hideProject,
               Provider<ReplicatedEventsCoordinator> replicatedEventsCoordinator) {
    super(allProjectsNameProvider, allUsersNameProvider, dbHandler, fsHandler, cacheHandler,
        pcHandler, userProvider, pluginName, deleteLog, cfgFactory, hideProject, replicatedEventsCoordinator);
  }

  @Override
  public UiAction.Description getDescription(ProjectResource rsrc) {
    boolean replicated = true;
    Logger log = LoggerFactory.getLogger(DeleteAction.class);

    try {
      replicated = DeleteProject.isRepoReplicated(rsrc.getName());
    } catch (IOException e) {
      log.error("Error accessing the project :", rsrc.getName(), " Error: ", e, " [ Assuming a replicated repo is in place. ]" );
    }
    return new UiAction.Description()
        .setLabel(replicated ? "Clean up..." : "Delete...")
        .setTitle(isAllNamedProject(rsrc) ?
            String.format("No deletion of %s or %s project", allProjectsName, allUsersName) :
              String.format("%s%sproject %s",
                  (replicated? "Clean up":"Delete"), (replicated? " replicated ":" "),
                  rsrc.getName()))
        .setEnabled(!isAllNamedProject(rsrc))
        .setVisible(canDelete(rsrc));
  }

  private boolean isAllNamedProject(ProjectResource rsrc) {
    String currentProject = rsrc.getControl().getProject().getName();
    return (currentProject.equals(allProjectsName)|| currentProject.equals(allUsersName));
  }
}
