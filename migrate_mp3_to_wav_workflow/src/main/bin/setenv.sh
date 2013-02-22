#!/bin/bash

SCRIPT_PATH=$(dirname $(readlink -f $0))

#Jhove2Install=
#ffmpegInstall
#migrationQAInstall=
#mpg321Install=$HOME/tools/
#tavernaInstall=

#iapetus
PATH=$PATH:$SCRIPT_PATH:$HOME/:$HOME/tools/taverna-commandline-2.4.0

#bam-local
#PATH=$PATH:$SCRIPT_PATH:$HOME/:/opt/taverna-workbench-2.4.0
