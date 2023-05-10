#!/bin/bash
if [ -z "${MINIO}" ]; then
  exit 0
fi

set -eux

/usr/bin/mc alias set minio http://${MINIO} minioadmin minioadmin
/usr/bin/mc cp /benchbase/results/* minio/neon/$1/
rm -r /benchbase/results/*