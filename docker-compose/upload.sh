#!/bin/bash
set -eux

/usr/bin/mc alias set local http://localhost:9000 minioadmin minioadmin
/usr/bin/mc cp /benchbase/results/* local/neon/$1/