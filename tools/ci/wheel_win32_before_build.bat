@echo on

pip install delvewheel wheel

REM The libpq triplet comes from the workflow matrix; x64 when it is not set.
if not defined VCPKG_TRIPLET set VCPKG_TRIPLET=x64-windows-release

REM A specific version cannot be easily chosen.
REM https://github.com/microsoft/vcpkg/discussions/25622
REM vcpkg-triplets\ adds arm64-windows-release, which vcpkg does not ship.
vcpkg install --overlay-triplets=%~dp0vcpkg-triplets libpq:%VCPKG_TRIPLET%

pipx install .\tools\ci\pg_config_vcpkg_stub\
