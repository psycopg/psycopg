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

- Delete the ``wheelhouse`` directory if there is one.

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

- If the script above found any change, document the version added at the
  bottom of ``docs/api/errors.rst``.

- Run the script ``tools/update_oids.py`` to add new oids. Use ``-h`` to get
  an example docker command line to run a server locally.

- Check if there are new enum values to include in:

  - ``psycopg_c/psycopg_c/pq/libpq.pxd``;
  - ``psycopg/psycopg/pq/_enums.py``.

- Include the new version in GitHub Actions test and package grids.

- Bump ``PG_VERSION`` in the ``macos`` job of

  -  ``.github/workflows/packages-bin.yml``.
  -  ``.github/workflows/tests.yml``.

- Bump ``pg_version`` in ``tools/build/build_macos_arm64.sh``.

- Bump the version in ``tools/build/wheel_win32_before_build.bat``.

- Update the documented versions in:

  - ``docs/basic/install.rst``;
  - ``content/features/contents.lr`` in the psycopg-website repository.


When a new Python major version is released
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Add the new version to the relevant test matrices in
  ``.github/workflows/tests.yml`` and ``.github/workflows/packages-bin.yml``.

- Update ``docs/basic/install.rst`` with the correct range of supported Python
  versions.

- Add the ``Programming Language :: Python :: 3.<X>`` classifier to
  ``psycopg/pyproject.toml``, ``psycopg_c/pyproject.toml``, and
  ``psycopg_pool/pyproject.toml``.

- Update the list of versions in ``tools/build/build_macos_arm64.sh`` to include
  the new version. Look for both the ``python_versions`` variable and the
  ``CIBW_BUILD`` environment variable.


When dropping end-of-life Python versions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Update project metadata, ``requires-python`` and (maybe) package dependencies
  in ``pyproject.toml`` files of the corresponding ``psycopg`` directories.

- Update GitHub Actions workflow files in the ``.github/workflows/`` directory,
  e.g., ``tests.yml``, ``.3rd-party-tests.yml``, ``packages-bin.yml``.

- Bump versions in the ``tests/constraints.txt`` file if it is necessary.

- You may grep throughout the project for occurrences of a version to be dropped.
  However, favouring smaller pull requests is convenient and easy to review.
  An example for grepping `end-of-life <https://endoflife.date/python>` Python 3.8::

     git grep -E -e '\b3\.8\b' -e '\b(cp)?38\b' -e '\b3, 8\b'
     git grep -E -e '\b3\.9\b' -e '\b(cp)?39\b' -e '\b3, 9\b'

- Consider using pyupgrade_ with ``--py3NN-plus`` in order to refresh syntax
  to Python 3.NN (the new minimum supported version).

.. _pyupgrade: https://pypi.org/project/pyupgrade/

Examples:

- `PR #977 <https://github.com/psycopg/psycopg/pull/977>`_
