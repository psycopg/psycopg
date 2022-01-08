Psycopg 3: PostgreSQL database adapter for Python - optimisation package
========================================================================

This distribution contains the optional optimization package ``psycopg_c``.

You shouldn't install this package directly: use instead ::

    pip install psycopg[c]

to install a version of the optimization package matching the ``psycopg``
version installed.

Installing this package requires some prerequisites: check `Local
installation`__ in the documentation. Without a C compiler and some library
headers install *will fail*: this is not a bug.

If you are unable to meet the prerequisite needed you might want to install
``psycopg[binary]`` instead: look for `Binary installation`__ in the
documentation.

.. __: https://www.psycopg.org/psycopg3/docs/basic/install.html
       #local-installation
.. __: https://www.psycopg.org/psycopg3/docs/basic/install.html
       #binary-installation

Please read `the project readme`__ and `the installation documentation`__ for
more details.

.. __: https://github.com/psycopg/psycopg#readme
.. __: https://www.psycopg.org/psycopg3/docs/basic/install.html


Copyright (C) 2020 The Psycopg Team
