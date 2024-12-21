@echo on

vcpkg install libpq:x64-windows-release

pipx install .\tools\build\pg_config_vcpkg_stub\
