@echo on

vcpkg install libpq:x64-windows-release


pip install .\tools\build\pg_config_stub\
