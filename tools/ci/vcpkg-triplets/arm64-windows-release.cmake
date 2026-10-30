# vcpkg has no built-in arm64 counterpart of x64-windows-release: the same triplet for Windows on Arm.
set(VCPKG_TARGET_ARCHITECTURE arm64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE dynamic)
set(VCPKG_BUILD_TYPE release)
set(VCPKG_PROVIDED_FORTRAN ON)
