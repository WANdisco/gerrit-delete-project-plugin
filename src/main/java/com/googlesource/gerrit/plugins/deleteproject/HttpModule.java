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

import com.google.gerrit.extensions.registration.DynamicSet;
import com.google.gerrit.extensions.webui.JavaScriptPlugin;
import com.google.gerrit.extensions.webui.WebUiPlugin;
import com.google.inject.Inject;
import com.google.inject.servlet.ServletModule;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class HttpModule extends ServletModule {
  private static final Logger log = LogManager.getLogger(HttpModule.class.getName());
  private final Configuration cfg;

  @Inject
  HttpModule(Configuration cfg) {
    this.cfg = cfg;
  }

  @Override
  protected void configureServlets() {
    if (!cfg.enablePreserveOption()) {
      log.warn("`enablePreserveOption=false` has been set in gerrit.config. This configuration will be ignored.");
    }

    DynamicSet.bind(binder(), WebUiPlugin.class)
              .toInstance(new JavaScriptPlugin("gr-delete-repo.html"));
    // Required for old UI
    DynamicSet.bind(binder(), WebUiPlugin.class)
              .toInstance(new JavaScriptPlugin("delete-project.js"));
  }
}
