
#/********************************************************************************
# * Copyright (c) 2014-2018 WANdisco
# *
# * Licensed under the Apache License, Version 2.0 (the "License");
# * you may not use this file except in compliance with the License.
# * You may obtain a copy of the License at
# * http://www.apache.org/licenses/LICENSE-2.0
# *
# * Apache License, Version 2.0
# *
# ********************************************************************************/
 
include_defs('//bucklets/gerrit_plugin.bucklet')

gerrit_plugin(
  name = 'gerrit-delete-project-plugin',
  srcs = glob(['src/main/java/**/*.java']),
  resources = glob(['src/main/resources/**/*']),
  manifest_entries = [
    'Gerrit-PluginName: deleteproject',
    'Gerrit-Module: com.googlesource.gerrit.plugins.deleteproject.Module',
    'Gerrit-HttpModule: com.googlesource.gerrit.plugins.deleteproject.HttpModule',
    'Gerrit-SshModule: com.googlesource.gerrit.plugins.deleteproject.SshModule',
  ],
  provided_deps = [
    '//lib:gson',
    '//lib/log:log4j',
  ],
)

java_library(
  name = 'classpath',
  deps = GERRIT_PLUGIN_API + [
    ':gerrit-delete-project-plugin',
  ],
)

java_test(
  name = 'delete-project_tests',
  srcs = glob(['src/java/**/*.java']),
  labels = ['delete-project'],
  deps = GERRIT_PLUGIN_API + GERRIT_TESTS + [':gerrit-delete-project-plugin'],
)
