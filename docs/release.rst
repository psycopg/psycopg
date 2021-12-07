:orphan:

How to make a psycopg release
=============================

- Change version number in:

  - ``psycopg_c/psycopg_c/version.py``
  - ``psycopg/psycopg/version.py``
  - ``psycopg_pool/psycopg_pool/version.py``

- Change docs/news.rst to drop the "unreleased" mark from the version

- Push to GitHub to run `the tests workflow`__.

  .. __: https://github.com/psycopg/psycopg/actions/workflows/tests.yml

- Build the packages by triggering manually the `Build packages workflow`__.

  .. __: https://github.com/psycopg/psycopg/actions/workflows/packages.yml

- If all went fine, create a tag named after the version::

    git tag -a -s 3.0.dev1
    git push --tags

- Download the ``artifacts.zip`` package from the last Packages workflow run.

- Unpack the packages locally::

    mkdir tmp
    cd tmp
    unzip ~/Downloads/artifact.zip

- If the package is a testing one, upload it on TestPyPI with::

    $ twine upload -s -r testpypi *

- If the package is stable, omit ``-r testpypi``.
