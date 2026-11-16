#!/usr/bin/env bash
set -euo pipefail

# Linker for cross-compilation targeting Windows 7 / Server 2008 R2 (MSVC).
# Uses an SDK unpacked via xwin. The SDK path comes from XWIN_SDK
# (defaults to ~/winsdk).

XWIN_SDK="${XWIN_SDK:-$HOME/winsdk}"

lld-link "$@" \
    /libpath:"$XWIN_SDK/crt/lib/x86_64" \
    /libpath:"$XWIN_SDK/sdk/lib/um/x86_64" \
    /libpath:"$XWIN_SDK/sdk/lib/ucrt/x86_64"
