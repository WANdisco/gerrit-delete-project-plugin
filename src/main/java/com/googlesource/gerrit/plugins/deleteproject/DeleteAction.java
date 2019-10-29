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

import com.google.gerrit.server.replication.ReplicatedProjectManager;
import com.google.gerrit.extensions.webui.UiAction;
import com.google.gerrit.server.CurrentUser;
import com.google.gerrit.server.notedb.NotesMigration;
import com.google.gerrit.server.project.ProjectResource;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.googlesource.gerrit.plugins.deleteproject.cache.CacheDeleteHandler;
import com.googlesource.gerrit.plugins.deleteproject.database.DatabaseDeleteHandler;
import com.googlesource.gerrit.plugins.deleteproject.fs.FilesystemDeleteHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DeleteAction extends DeleteProject implements
    UiAction<ProjectResource> {
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
      HideProject hideProject,
      NotesMigration migration) {
    super(
        dbHandler,
        fsHandler,
        cacheHandler,
        userProvider,
        deleteLog,
        preConditions,
        cfg,
        hideProject,
        migration);
    this.protectedProjects = protectedProjects;
  }

  @Override
  public UiAction.Description getDescription(ProjectResource rsrc) {
    boolean replicated = true;
    Logger log = LoggerFactory.getLogger(ReplicatedProjectManager.class);

    try {
      replicated = DeleteProject.isRepoReplicated(rsrc);
    } catch (IOException e) {
      log.error("Error accessing the project :", rsrc.getName(), " Error: ", e, " [ Assuming a replicated repo is in place. ]" );
    }
    return new UiAction.Description()
        .setLabel(replicated ? "Clean up..." : "Delete...")
        .setTitle(isAllProjects(rsrc) ?
            String.format("No deletion of %s project", allProjectsName) :
              String.format("%s%sproject %s",
                  (replicated? "Clean up":"Delete"), (replicated? " replicated ":" "),
                  rsrc.getName()))
        .setEnabled(!isAllProjects(rsrc))
        .setVisible(canDelete(rsrc));
  }
}
