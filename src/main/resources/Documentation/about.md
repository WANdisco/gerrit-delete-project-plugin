
Provides the ability to delete a project.

Gerrit stores project information both on disk and in the database.
The delete project plugin has been created to help wipe this data.

There are several reasons you may want to delete a project including:

* The project has reached the end of its life and needs to be completely removed, but a backup is required for reference/future use

* The project has become hard to manage, there are too many outdated/abandoned reviews and it needs to be cleaned up

Using the delete project plugin
-----------

Depending on whether your project is replicated or non-replicated the plugin UI will look different and have different behaviors.

* A **Replicated Project** will be present on disk on all nodes of the replication group it is a member of, in both the GitMS UI and the Gerrit UI.
If you are deleting a Replicated Project there a 2 options, preserve and no preserve. These options are under the **Clean up** command.

	**Remove Gerrit project data (leave repository replicating in GitMS)**
	If you select this option all associated changes, reviews etc will be removed, the project will remain in the project list on Gerrit, on disk and will continue to replicate within GitMS. It is a clean up of the project rather than complete removal. You will still be able to checkout the repo and commit changes.

	**Remove Gerrit project data, remove from replication and archive repository**
	If you select this option you will completely remove the repository. The project and all associated changes, reviews etc will be removed from Gerrit, the repo will be removed from replication in GitMS and will no longer exist on disk.
	However, before removal a zipped version of the repo will be archived to a directory specified during install.

* A **Non-Replicated Project** is not replicated across a set of nodes and so will likely only appear on one node, both on disk and in the Gerrit UI, but NOT within GitMS.
	As you are deleting a non-replicated project this is the *original* delete project plugin functionality.
	The command for these projects is **Delete**.

	When a non-replicated project is fully deleted, a project deletion event is fired.
	Other plugins can listen to this event by implementing `com.google.gerrit.extensions.events.ProjectDeletedListener` which is part of the Gerrit core extension API.
	The project deletion event is only fired if the Git repository of the project is deleted.

Limitations when deleting a non-replicated project
-----------

There are a few caveats:

* This cannot be undone

	This is an irreversible action, and should be taken with extreme care. Backups are always advised of any important data.

* You cannot delete projects that use "submodule subscription"

	If deleting a project that makes use of submodule subscription, you cannot delete the project. Remove the submodule registration before attempting to delete the project.

Access
------

To be allowed to delete arbitrary projects a user must be a member of a group that is granted the 'Delete Project' capability (provided by this plugin) or the 'Administrate Server' capability. Project owners are allowed to delete their own projects if they are member of a group that is granted the 'Delete Own Project' capability (provided by this plugin).