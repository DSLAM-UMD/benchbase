#!/bin/bash
set -eux

/usr/bin/mc alias set minio http://${MINIO} minioadmin minioadmin
/usr/bin/mc cp /benchbase/results/* minio/neon/$1/