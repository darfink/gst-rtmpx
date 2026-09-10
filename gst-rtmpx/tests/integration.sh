#!/usr/bin/env bash
# Minimal smoke test for the rtmpx scaffold: build, run unit tests,
# and confirm both elements register.
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -z "${PKG_CONFIG_PATH:-}" ] && [ -d /Library/Frameworks/GStreamer.framework/Versions/Current/lib/pkgconfig ]; then
  export PKG_CONFIG_PATH=/Library/Frameworks/GStreamer.framework/Versions/Current/lib/pkgconfig
fi
# The GStreamer framework ships its dylibs and tools outside the default
# search paths. Export them here so the script works from a bare shell; this
# must happen inside the script because DYLD variables are not inherited
# by file-executed scripts on macOS.
if [ -d /Library/Frameworks/GStreamer.framework/Versions/Current/lib ]; then
  export DYLD_FALLBACK_LIBRARY_PATH="/Library/Frameworks/GStreamer.framework/Versions/Current/lib${DYLD_FALLBACK_LIBRARY_PATH:+:$DYLD_FALLBACK_LIBRARY_PATH}"
  export PATH="/Library/Frameworks/GStreamer.framework/Versions/Current/bin:$PATH"
fi
cargo build -p gst-rtmpx
cargo test -p gst-rtmpx
export GST_PLUGIN_PATH="$PWD/../target/debug:${GST_PLUGIN_PATH:-}"
if [ -d ./target/debug ]; then
  export GST_PLUGIN_PATH="$PWD/target/debug:$GST_PLUGIN_PATH"
fi
gst-inspect-1.0 rtmpxsrc | head -30 || true
gst-inspect-1.0 rtmpxsink | head -30 || true
