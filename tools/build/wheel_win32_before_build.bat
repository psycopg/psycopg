@echo on

vcpkg install libpq:x64-windows-release


pip install .\tools\build\pg_config_vcpkg_stub\

pg_config.exe --help
