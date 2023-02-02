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
import com.google.gerrit.extensions.webui.UiAction;
import com.google.gerrit.server.CurrentUser;
import com.google.gerrit.server.project.ProjectResource;
import com.wandisco.gerrit.gitms.shared.config.ReplicationConfiguration;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.googlesource.gerrit.plugins.deleteproject.cache.CacheDeleteHandler;
import com.googlesource.gerrit.plugins.deleteproject.database.DatabaseDeleteHandler;
import com.googlesource.gerrit.plugins.deleteproject.fs.FilesystemDeleteHandler;

import java.io.IOException;

public class DeleteAction extends DeleteProject implements UiAction<ProjectResource> {
  private static final FluentLogger log = FluentLogger.forEnclosingClass();

  private final ProtectedProjects protectedProjects;

  @Inject
  DeleteAction(
      ProtectedProjects protectedProjects,
      DatabaseDeleteHandler dbHandler,
      FilesystemDeleteHandler fsHandler,
      CacheDeleteHandler cacheHandler,
      Provider<CurrentUser> userProvider,
      DeleteLog deleteLog,
      DeletePreconditions preConditions,
      Configuration cfg,
      HideProject hideProject) {
    super(
        dbHandler,
        fsHandler,
        cacheHandler,
        userProvider,
        deleteLog,
        preConditions,
        cfg,
        hideProject);
    this.protectedProjects = protectedProjects;
  }

  @Override
  public UiAction.Description getDescription(ProjectResource rsrc) {
    boolean isReplicatedRepo;
    try {
      isReplicatedRepo = DeleteProject.isRepoReplicated(rsrc.getName());
    } catch (IOException e) {
      log.atSevere().log("Could not discern whether [%s] repository is replicated. Disabling use of delete-project " +
                         "plugin for this repository - %s", rsrc.getName(), e.getMessage());
      return new UiAction.Description()
              .setEnabled(false)
              .setVisible(false);
    }

    String title;
    boolean enabled = true;

    // this should take precedence over other title (tooltip) variations
    if (isReplicatedRepo && ReplicationConfiguration.isReplicationDisabled()) {
      title = "WARNING: Replicated repository cannot be modified during non-replicated invocation of Gerrit.";
      enabled = false;
    } else if (protectedProjects.isProtected(rsrc)) {
      title = String.format("Not allowed to delete %s", rsrc.getName());
      enabled = false;
    } else {
      title = String.format("%s project %s", (isReplicatedRepo ? " Delete replicated": "Delete"), rsrc.getName());
    }

    return new UiAction.Description()
        .setLabel(isReplicatedRepo ? "Delete replicated project..." : "Delete project...")
        .setTitle(title)
        .setEnabled(enabled)
        .setVisible(preConditions.canDelete(rsrc));
  }
}
