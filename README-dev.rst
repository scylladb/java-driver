Building the Docs
=================

*Note*: The docs build instructions have been tested with Sphinx 2.4.4 and Fedora 32.

To build and preview the docs locally, you will need to install the following software:

- `Git <https://git-scm.com/book/en/v2/Getting-Started-Installing-Git>`_
- `Python 3.7 <https://www.python.org/downloads/>`_
- `pip <https://pip.pypa.io/en/stable/installing/>`_
-  Java JDK 6 or above
-  Maven

Run the following command to build the docs.

.. code:: console

    cd docs
    make preview

Once the command completes processing, open http://127.0.0.1:5500/ with your preferred browser.

To know more about how docs are generated, see `Sphinx ScyllaDB Theme <https://sphinx-theme.scylladb.com/>`_. 

Building multiple documentation versions
========================================

Build Sphinx docs for all the versions defined in `docs/conf.py <https://sphinx-theme.scylladb.com/stable/configuration.html#defining-supported-versions>`_.

.. code:: console

    cd docs
    make multiversionpreview

Once the command completes processing, open http://0.0.0.0:5500/ with your preferred browser.

**NOTE:** If you only can see docs generated for the master branch, try to run ``git fetch --tags`` to download the latest tags from remote.

Defining supported versions
===========================

This repository mirrors DataStax's `java-driver <https://github.com/datastax/java-driver>`_ with custom features and bug fixes to be compatible with ScyllaDB.

Every time there is a new upstream version, the DataStax repository changes are taken as the base, and the previous custom commits are cherry-picked on top.
This workflow differs from how other forked driver repositories are maintained, where all changes from upstream are integrated into a default branch (typically master or latest).

In practice, we need to port the documentation generator and custom changes done for every new release, as we do with code changes that are not available in the latest upstream version.

**Steps**

Let's say you want to generate docs for the version ``1.0-scylla``.
You have already ported the custom ScyllaDB changes to the target branch, but you want to publish HTML docs for the new version.

1. Copy the next files from ``latest`` to the branch you want to enable docs generation. For our case, this would be a branch named ``1.0-scylla``.

    * ``docs``: Contains the documentation generation system and how docs are structured.
    * ``README.md``: A custom page describing the specifics for the ScyllaDB plugin.

2. Commit & push the copied files into the target branch. Make sure the docs build without errors:

    .. code:: console

        cd docs
        make preview

3. Move back to the ``latest`` branch. List the target version in ``docs/conf.py``.

    .. code:: python

        BRANCHES = ['1.0-scylla']
        smv_branch_whitelist = multiversion_regex_builder(BRANCHES)

4. Commit & push the changes to the ``latest`` branch.