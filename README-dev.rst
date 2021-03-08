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

Building multiple documentation versions
========================================

Build Sphinx docs for all the versions defined in ``docs/conf.py``.

.. code:: console

    cd docs
    make multiversion

Then, open ``docs/_build/dirhtml/<version>/index.html`` with your preferred browser.

**NOTE:** If you only can see docs generated for the master branch, try to run ``git fetch --tags`` to download the latest tags from remote.

Defining supported versions
===========================

Let's say you want to generate docs for the new version ``scylla-3.x.y``.

1. The file ``.github/workflows`` defines the branch from where all the documentation versions will be build.

.. code:: yaml

    on:
    push:
        branches:
        - scylla-3.x

In our case, this branch currently is``scylla-3.x``.
In practice, this means that the file ``docs/source/conf.py`` of ```scylla-3.x`` defines which documentation versions are supported.

2. In the file ``docs/source/conf.py`` (``scylla-3.x`` branch), list the new target version support inside the ``BRANCHES`` array.
For example, listing ``scylla-3.x.y`` should look like in your code:

.. code:: python

    BRANCHES = ['scylla-3.x.y']
    smv_branch_whitelist = multiversion_regex_builder(BRANCHES)

3. (optional) If the new version is the latest stable version, update as well the variable ``smv_latest_version`` in ``docs/source/conf.py``.

.. code:: python

    smv_latest_version = 'scylla-3.x.y'

4. Commit & push the changes to the ``scylla-3.x`` branch.
