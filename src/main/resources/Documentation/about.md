The WANdisco fork of the delete-project plugin provides the ability to:

* Remove Gerrit project data, remove from GitMS replication and archive the Git Repository.

or
* Remove Gerrit project data but leave it replicating within GitMS.

There is also the ability to force the removal of open changes if they exist
with either of these options.

Deleting a project means that the project is completely removed from
the Gerrit installation, including all its changes and optionally its
Git repository.

##**Remove Gerrit project data, remove from GitMS replication and archive the Git Repository.**

This option is for a full delete of the Git Repository. When a project is fully deleted, a
project deletion event is fired. This event is replicated via GitMS to all other sites in a replication
group. GitMS will archive the Git repository at all sites. GitMS will only remove the Git repository after 
it has been successfully archived. With the Git repository having been archived, it is possible to bring the
repository back into Gerrit.

For the delete-project plugin, there are two properties of interest in the application.properties for GitMS
 
    gerrit.repo.home:
    * The Git repository will be removed from disk in the location specified
    by the property 'gerrit.repo.home' in the application.properties of GitMS.
    The default location is GERRIT_SITE/git

    deleted.repo.directory:
    * The Git repository is archived at all sites in the replication
    group in the location specified by the property 'deleted.repo.directory'. 
    The default archive directory location is : GERRIT_SITE/git/archiveOfDeletedGitRepositories


##**Removing project data, but leaving the project replicating within GitMS** 

Selecting this option means that only the project data itself is removed. The Git Repository 
will not be removed. This option is the `preserve` option in the WANdisco fork of the delete-project 
plugin and will always be enabled therefore `enablePreserveOption` is not configurable. 
See config.md for more information.

Warning: A reindex of the repository will bring back all the removed changes.


##**Force cleanup of open changes**

When selected, upon deleting the project, this will remove any open changes. This can be used alongside the
two primary options described above.

Other plugins can listen to this event by implementing
`com.google.gerrit.extensions.events.ProjectDeletedListener` which is
part of the Gerrit core extension API. The project deletion event is
only fired if the Git repository of the project is deleted.

Limitations
-----------

caveats:

* You cannot delete projects that use "submodule subscription"

	If deleting a project that makes use of submodule subscription,
	you cannot delete the project. Remove the submodule registration
	before attempting to delete the project.

Replication of project deletions
--------------------------------
Replication of project deletions is performed by GitMS. The event that is
triggered upon a project deletion is replicated to all other sites
in a given replication group via GitMS.

The [replication plugin]
(https://gerrit-review.googlesource.com/#/admin/projects/plugins/replication)
can be configured to listen to the project deletion event and to
replicate project deletions.

Access
------

To be allowed to delete arbitrary projects a user must be a member of a
group that is granted the 'Delete Project' capability (provided by this
plugin) or the 'Administrate Server' capability. Project owners are
allowed to delete their own projects if they are member of a group that
is granted the 'Delete Own Project' capability (provided by this
plugin).

