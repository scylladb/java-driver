Building the Docs
=================

*Note*: The docs build instructions have been tested with Sphinx 2.4.4 and Fedora 32.

To build and preview the theme locally, you will need to install the following software:

- `Git <https://git-scm.com/book/en/v2/Getting-Started-Installing-Git>`_
- `Python 3.7 <https://www.python.org/downloads/>`_
- `pip <https://pip.pypa.io/en/stable/installing/>`_
- `doxygen <https://www.tutorialspoint.com/how-to-install-doxygen-on-ubuntu/>`_

Run the following command to build the docs.

.. code:: console

    cd docs
    make preview

Once the command completes processing, open http://127.0.0.1:5500/ with your preferred browser.

Building multiple documentation versions
========================================

Build Sphinx docs for all the versions defined in ``docs/conf.py``.

The multiverson command does not build doxygen docs.

```
cd docs
make multiversion
```

Then, open ``docs/_build/dirhtml/<version>/index.html`` with your preferred browser.

**NOTE:** If you only can see docs generated for the master branch, try to run ``git fetch --tags`` to download the latest tags from remote.
