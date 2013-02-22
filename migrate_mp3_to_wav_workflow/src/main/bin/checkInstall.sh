#!/bin/bash

invalid=0

SCRIPT_PATH=$(dirname $(readlink -f $0))

source $SCRIPT_PATH/setenv.sh

ffprobeVersion=$(ffprobe -version | head -1 | cut -d' ' -f3 | cut -d'-' -f1)
if [[ "$ffprobeVersion" < "0.10" ]]; then
    echo "FFProbe is of to old version, $ffprobeVersion, require 0.10.0"
    invalid=1
fi

mpg321Version=$(mpg321 --version | head -1 | cut -d' ' -f3 | cut -d'-' -f1)
if [[ "$mpg321Version" < "0.2.0" ]]; then
    echo "mpg321 is of to old version, $mpg321Version, require 0.2.0"
    invalid=1
fi

migrationQAVersion=$(migrationQA --version | head -1 | cut -d' ' -f3 | cut -d'-' -f1)
if [[ "$migrationQAVersion" < "0.1.0" ]]; then
    echo "migrationQA (later changed to waveform-compare) is of to old version, $migrationQAVersion, require 0.1.0"
    invalid=1
fi



jhove2.sh --help 2>&1 > /dev/null
jhove2Installed=$?
if [ $jhove2Installed != 0 ]; then
    echo "JHove2 characterisation framework engine not installed"
    invalid=1
fi


executeworkflow.sh 2>&1 > /dev/null
tavernaInstalled=$?
if [ $tavernaInstalled != 0 ]; then
    echo "Taverna workflow engine not installed"
    invalid=1
fi

exit $invalid

