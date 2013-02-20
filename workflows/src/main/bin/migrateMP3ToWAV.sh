#!/bin/bash

CURRENT=$PWD

SCRIPT_PATH=$(dirname $(readlink -f $0))

WORKFLOW_PATH=$(readlink -f $SCRIPT_PATH/../taverna)
WORKFLOW="$WORKFLOW_PATH/migrateValidateCompare_iapetus_listToList_largeScaleTest_V2.t2flow"

MP3_LIST=$1
OUTPUT_DIR=$2

if [ -z "$OUTPUT_DIR" ]; then
    OUTPUT_DIR=$CURRENT
fi

if [ "$(ps -ef | grep [\ ]$WORKFLOW)" != "" ] ; then
    echo "Workflow was already was running, exiting!"
    exit 1
fi

if [ -r "$SCRIPT_PATH"/setenv.sh ]; then
    source "$SCRIPT_PATH"/setenv.sh
fi



pushd $SCRIPT_PATH > /dev/null

./checkInstall.sh

cleanup () {
   rm -rf "$TAVERNA_TEMP_DIR"
   popd > /dev/null
}
trap cleanup 0 3 15

TAVERNA_TEMP_DIR=$(mktemp -d -p "$HOME/tmp/taverna")

export TMPDIR="$TAVERNA_TEMP_DIR"
export TEMPDIR="$TAVERNA_TEMP_DIR"
export TMP="$TAVERNA_TEMP_DIR"
export TEMP="$TAVERNA_TEMP_DIR"
export _JAVA_OPTIONS="-Djava.io.tmpdir=$TAVERNA_TEMP_DIR"

export TAVERNA_SCRIPT_DIR=$SCRIPT_PATH

executeworkflow.sh \
-inmemory \
-inputvalue mp3_list "$MP3_LIST"  \
-inputvalue output_files "$OUTPUT_DIR"  \
-outputdir "$OUTPUT_DIR/taverna" \
"$WORKFLOW"


cleanup