#!/usr/bin/env bash
set -euo pipefail

# Линкер для кросс-сборки под Windows 7 / Server 2008 R2 (MSVC).
# Использует SDK, распакованный через xwin. Путь к SDK берётся из XWIN_SDK
# (по умолчанию ~/winsdk).

XWIN_SDK="${XWIN_SDK:-$HOME/winsdk}"

lld-link "$@" \
    /libpath:"$XWIN_SDK/crt/lib/x86_64" \
    /libpath:"$XWIN_SDK/sdk/lib/um/x86_64" \
    /libpath:"$XWIN_SDK/sdk/lib/ucrt/x86_64"
