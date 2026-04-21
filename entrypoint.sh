#!/usr/bin/env sh
set -e

mkdir -p /data

if [ -d /opt/tessdata-base ]; then
  cp -n /opt/tessdata-base/*.traineddata /data/ 2>/dev/null || true
fi

exec "$@"