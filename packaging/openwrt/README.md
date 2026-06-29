OpenWrt packaging for oxidns

This directory contains a minimal OpenWrt package skeleton for oxidns.

Files included:
- Makefile: OpenWrt package Makefile that installs files from the files/ directory.
- files/etc/init.d/oxidns: procd init script (executable)
- files/etc/config/oxidns: UCI configuration sample
- files/usr/bin/README: where to place prebuilt binaries

Usage notes
1. Prefer CI-built, architecture-specific static binaries (musl) and attach them as GitHub Release assets.
2. Add this directory to an OpenWrt SDK under package/ and run `make package/oxidns/compile V=s` to build the .ipk.
3. Alternatively, modify the Makefile to implement Build/Compile to build from source inside the SDK.

If you want, I can also add a GitHub Actions workflow to build static binaries for common architectures and publish release assets.
