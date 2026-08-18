@echo on

pip install delvewheel wheel

REM The workflows set VCPKG_TRIPLET; default to the native arch otherwise.
if "%VCPKG_TRIPLET%"=="" (
    if "%PROCESSOR_ARCHITECTURE%"=="ARM64" (
        set VCPKG_TRIPLET=arm64-windows
    ) else (
        set VCPKG_TRIPLET=x64-windows-release
    )
)

REM A specific version cannot be easily chosen.
REM https://github.com/microsoft/vcpkg/discussions/25622
vcpkg install libpq:%VCPKG_TRIPLET%

pipx install .\tools\ci\pg_config_vcpkg_stub\
