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

if (!window.Polymer) {
  Gerrit.install(function(self) {
    function onDeleteProject(c) {
      var f = c.checkbox();
      var p = c.checkbox();
      var preserveSelect = null;

      var replicatedMsg = "";
      var forceDeleteMsg = "Delete project even if open changes exist?";
      var preserveDeleteMsg = "Preserve GIT Repository?";
      var archiveAndRemoveMsg = "";
      var isReplicated = false;
      var archiveAndDeleteOptions = [];

      //If the project is replicated we need to change the values of the messages in the pop-up and we need to
      //use a drop down rather than check boxes because the user can only select one element at a time
      if (c.action.title && c.action.title.includes("Clean up replicated project")) {
          isReplicated = true;
          replicatedMsg = " replicated";
          preserveDeleteMsg = "Remove Gerrit project data (leave repository replicating in GitMS)";
          archiveAndRemoveMsg = "Remove Gerrit project data, remove from replication and archive repository";
          archiveAndDeleteOptions = [preserveDeleteMsg, archiveAndRemoveMsg]
      }

      // If the project is replicated, show our new version of the pop-up, if not show the original
      if (isReplicated){
    	  var b = c.button('Clean Up',
    		{onclick: function(){
    		  var selectedOption =  c.selected(preserveSelect);
    		  var preserveProject = false;
    		  preserveProject = (selectedOption == preserveDeleteMsg);
    		  c.call(
    		    {force: f.checked, preserve: preserveProject},
    		    function(r) {
    		      c.hide();
    		      window.alert('The project: "'
    		        + c.project
    		        + '" was cleaned up.'),
    		      Gerrit.go('/admin/projects/');
    		    });
    		}});

    	  c.popup(c.div(
    	    		c.msg('Are you really sure you want to delete the'+replicatedMsg+' project: "'
    	    	    + c.project
    	    	    + '"?'),
    	    	    c.br(),
    	    	    c.label(f, forceDeleteMsg),
    	    	    c.br(),
    	    	    c.br(),
    	    	    c.msg("Preserve Options:"),
    	    	    c.br(),
    	    	    preserveSelect = c.select(archiveAndDeleteOptions, 0),//If 0 is selected then preserve = true. If 1 is selected then preserve = false
    	    	    c.br(),
    	    	    c.br(),
    	    	    b));
      }else{
    	  var b = c.button('Delete',
    		{onclick: function(){
    		  c.call(
    		    {force: f.checked, preserve: p.checked},
    		    function(r) {
    		      c.hide();
    		      window.alert('The project: "'
    		        + c.project
    		        + '" was deleted.'),
    		      Gerrit.go('/admin/projects/');
    		    });
    		}});

    	  c.popup(c.div(
    	    		c.msg('Are you really sure you want to delete the project: "'
    	    	    + c.project
    	    	    + '"?'),
    	    	    c.br(),
    	    	    c.label(f, forceDeleteMsg),
    	    	    c.br(),
    	    	    c.label(p, preserveDeleteMsg),
    	    	    c.br(),
    	    	    b));
      }

    }

    self.onAction('project', 'delete', onDeleteProject);
  });
}

  function onDeleteProject(c) {
    var f = c.checkbox();
    var p = c.checkbox();
    var b = c.button('Delete',
      {onclick: function(){
        c.call(
          {force: f.checked, preserve: p.checked},
          function(r) {
            c.hide();
            window.alert('The project: "'
              + c.project
              + '" was deleted.'),
            Gerrit.go('/admin/projects/');
          });
      }});
    c.popup(c.div(
      c.msg('Are you really sure you want to delete the project: "'
        + c.project
        + '"?'),
      c.br(),
      c.label(f, 'Delete project even if open changes exist?'),
      c.br(),
      c.label(p, 'Preserve GIT Repository?'),
      c.br(),
      b));
  }
  self.onAction('project', 'delete', onDeleteProject);
});
