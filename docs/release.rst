:orphan:

How to make a psycopg release
=============================

- Check if there is a new version or libpq_ or OpenSSL_; in such case
  update ``LIBPQ_VERSION`` and/or ``OPENSSL_VERSION`` in
  ``.github/workflows/packages-bin.yml``.

    .. _libpq: https://www.postgresql.org/ftp/source/

    .. _OpenSSL: https://www.openssl.org/source/

- Check if there is a new `cibuildwheel release`__; if so, upgrade it in
  ``.github/workflows/packages-bin.yml``.

  .. __: https://github.com/pypa/cibuildwheel/releases

- Use ``tools/bump_version.py`` to upgrade package version numbers.

- Push to GitHub to run `the tests workflow`__.

  .. __: https://github.com/psycopg/psycopg/actions/workflows/tests.yml

- Build the packages by triggering manually the ones requested among:

  - `Source packages`__
  - `Binary packages`__
  - `Pool packages`__

  .. __: https://github.com/psycopg/psycopg/actions/workflows/packages-src.yml
  .. __: https://github.com/psycopg/psycopg/actions/workflows/packages-bin.yml
  .. __: https://github.com/psycopg/psycopg/actions/workflows/packages-pool.yml

- Delete the ``wheelhouse`` directory there is one.

- Build m1 packages by running ``./tools/build/run_build_macos_arm64.sh BRANCH``.
  On successful completion it will save built packages in ``wheelhouse``

- If all packages were built ok, push the new tag created by ``bump_version.py``::

    git push --tags

- Download the ``artifacts.zip`` package from the last Packages workflow run.

- Unpack the packages in the wheelhouse dir::

    mkdir -p wheelhouse
    cd wheelhouse
    unzip ~/Downloads/artifact.zip

- If the package is a testing one, upload it on TestPyPI with::

    $ twine upload -r testpypi *

- If the package is stable, omit ``-r testpypi``::

    $ twine upload *

- Run ``tools/bump_version.py -l dev`` to bump to the next dev version.


When a new PostgreSQL major version is released
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Add the new version to ``tools/update_errors.py`` and run the script to add
  new error classes.

- Run the script ``tools/update_oids.py`` to add new oids. Use ``-h`` to get
  an example docker command line to run a server locally.

- Check if there are new enum values to include in:

  - ``psycopg_c/psycopg_c/pq/libpq.pxd``;
  - ``psycopg/psycopg/pq/_enums.py``.

- Include the new version in GitHub Actions test and package grids.

- Bump ``PG_VERSION`` in the ``macos`` job of
  ``.github/workflows/packages-bin.yml``.

- Bump ``pg_version`` in ``tools/build/build_macos_arm64.sh``.

- Bump the version in ``tools/build/wheel_win32_before_build.bat``.

- Update the documented versions in:

  - ``docs/basic/install.rst``;
  - ``content/features/contents.lr`` in the psycopg-website repository.
