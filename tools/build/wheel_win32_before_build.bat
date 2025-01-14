@echo on

pip install delvewheel wheel

REM A specific version cannot be easily chosen.
REM https://github.com/microsoft/vcpkg/discussions/25622
vcpkg install libpq:x64-windows-release

pipx install .\tools\build\pg_config_vcpkg_stub\
