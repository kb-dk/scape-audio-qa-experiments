#!/bin/bash
set -e
set -o pipefail

invalid=0

ffprobeVersion=$(ffprobe -version | head -1 | cut -d' ' -f3 | cut -d'-' -f1)
if [ "$ffprobeVersion" < "0.10.0" ]; then
    echo "FFProbe is of to old version, $ffprobeVersion, require 0.10.0"
    invalid=1
fi

mpg321Version=$(mpg321 --version | head -1 | cut -d' ' -f3 | cut -d'-' -f1)
if [ "$mpg321Version" < "0.2.0" ]; then
    echo "mpg321 is of to old version, $mpg321Version, require 0.2.0"
    invalid=1
fi

migrationQAVersion=$(migrationQA --version | head -1 | cut -d' ' -f3 | cut -d'-' -f1)
if [ "$migrationQAVersion" < "0.1.0" ]; then
    echo "migrationQA is of to old version, $migrationQAVersion, require 0.1.0"
    invalid=1
fi



jhove2.sh --help > /dev/null
jhove2Installed=$?
let "invalid = (( invalid || jhove2Installed ))"


executeWorkflow.sh > /dev/null
tavernaInstalled=$?
let "invalid = (( invalid || tavernaInstalled ))"

exit $invalid

