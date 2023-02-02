# WANdisco replicated delete-project plugin for Gerrit Code Review

A plugin which allows projects to be deleted from Gerrit via an SSH command,
REST API or the project settings screen.

Gerrit stores project information both on disk and in the database. 
A plugin called delete project has been created to help wipe this data. 
You will need to use this if you want to delete a project from Gerrit.

There are several reasons you may want to delete a project including:

* The project has reached the end of its life and needs to be completely removed, but a backup is required for reference/future use

* The project has become hard to manage, there are too many outdated/abandoned reviews and it needs to be cleaned up

## Install the delete-project plugin

Once you have installed GerritMS (version 1.10 or above) on all nodes, to install the delete-project plugin,  
run the following command. GerritMS must already be installed prior to installing the plugin.

```java -jar gerrit.war init -d <SITE_PATH> --install-plugin=gerrit-delete-project-plugin --batch```

Run this command on all nodes on which you want to be able to run the delete project plugin. Then restart GerritMS to complete the plugin installation.


## Using delete project plugin

The plugin acts differently depending on whether you are deleting a replicated or non replicated project.

* A replicated project will be present on disk on all nodes of the replication group it is a member of, in both the GitMS UI and the Gerrit UI.
If you are deleting a replicated project there a 2 options, preserve and no preserve.

    * ##### *Remove Gerrit project data (leave repository replicating in GitMS)*
      
      >If you select this option all associated changes & reviews will be removed. 
      >The project will remain in the project list on Gerrit, on disk and will continue to replicate within GitMS. 
      >It is a clean up of the project rather than complete removal. You will still be able to checkout the repository and commit changes.
      
    * ##### *Remove Gerrit project data, remove from replication and archive repository*
      
      >If you select this option you will completely remove the repository. The project and all associated changes, 
      >reviews etc will be removed from Gerrit, the repository will be removed from replication in GitMS and will no longer exist on disk.
      >However, before removal a zipped version of the repository will be archived to the directory specified during install.

* A non-replicated project is not replicated across a set of nodes and so will likely only appear on one node, both on disk and in the Gerrit UI, but NOT within GitMS.
If you are deleting a non-replicated project, the functionality is the same as the original delete-project plugin.


#### *Node goes down*

If a node goes down during the removal process, then when it comes back up and reloads the project cache, 
the project may still be listed on the GerritMS UI. If this happens then just flush the project cache.

#### *Deleted repositories directory*
GerritMS will never fully remove a project from disk, it will instead be backed up and archived. 
To delete projects you will need to have created an appropriate archive directory during install. 
The default location is /home/wandisco/gerrit/git/archiveOfDeletedGitRepositories.

You should periodically review the repositories that are in the directory and physically remove those that are no longer needed.

#### *If deletion fails*
If, for example, a node is down when you use the delete-project plugin, then deletion may fail. 
In this scenario you will need to perform a manual cleanup.

You will get a 500 server error in Gerrit, and on the GitMS dashboard, a failed task will appear 
(this will only be on the node you used the plugin from), to alert you that deletion has failed.

Deletion failure creates a file in the repository (WD_GitMS_CRITICAL_README) and an archived zip file. To cleanup follow these steps:

* Remove the WD_GitMS_CRITICAL_README file from the repository you tried to delete and the archived zip file from the deleted repositories directory. 
This needs to be done on all nodes in the replication group.

* Take the project out of Global Read Only status in GitMS.

* Ensure the issue preventing the initial failure is fixed, for example all nodes are up and running without problems.

* Run the delete-project plugin again.


[![Build Status](https://gerrit-ci.gerritforge.com/view/Plugins-stable-2.16/job/plugin-delete-project-bazel-stable-2.16/badge/icon
)](https://gerrit-ci.gerritforge.com/view/Plugins-stable-2.16/job/plugin-delete-project-bazel-stable-2.16/) 
