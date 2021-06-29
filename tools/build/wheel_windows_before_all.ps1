#!/bin/bash

# Configure the environment needed to build wheel packages on Windows.
# This script is designed to be used by cibuildwheel as CIBW_BEFORE_ALL_WINDOWS

# Set-PSDebug -Trace 1

python -c "import os; print(os.environ['PATH'])"

# choco install postgresql13 --params '/Password:password'

# From: https://www.enterprisedb.com/download-postgresql-binaries
Invoke-WebRequest `
    -Uri "https://sbp.enterprisedb.com/getfile.jsp?fileid=1257716" `
    -OutFile C:\postgresql-13.3-2-windows-x64-binaries.zip

Expand-Archive `
    -LiteralPath C:\postgresql-13.3-2-windows-x64-binaries.zip `
    -DestinationPath C:\

# python -c "import os; print(os.environ['PATH'])"

# pg_config

# dir C:/STRAWB~1/c/bin
# dir C:/STRAWB~1/c/lib

dir C:\pgsql\bin
C:\pgsql\bin\pg_config
