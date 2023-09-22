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

- Change ``docs/news.rst`` to drop the "unreleased" mark from the version

- Push to GitHub to run `the tests workflow`__.

  .. __: https://github.com/psycopg/psycopg/actions/workflows/tests.yml

- Build the packages by triggering manually the `Build packages workflow`__.

  .. __: https://github.com/psycopg/psycopg/actions/workflows/packages.yml

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
