@echo on
pip install delvewheel

REM I really want to write "REM", like when I had a C=64
REM (I am joking of course: I never wrote a comment when I had a C=64)

REM Broken since 2023-05-21, Failing with the error:
REM
REM   postgresql (exited 1) - postgresql not installed. An error occurred during
REM   installation: Unable to resolve dependency 'postgresql15 (= 15.3)'.
REM
REM Weeks later the error changed:
REM
REM   Unable to resolve dependency 'postgresql15': Unable to resolve
REM   dependencies. REM   'postgresql15 15.0.1' is not compatible with
REM   'postgresql 15.3.0 constraint: postgresql15 (= 15.3.0)'.
REM
REM choco upgrade postgresql

REM On https://community.chocolatey.org/packages/postgresql15/15.0.1#discussion
REM I found the following command in a comment:
choco install postgresql15 --version 15.0.1
REM which I'm going to randomly try.
