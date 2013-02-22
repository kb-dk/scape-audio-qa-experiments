#!/bin/bash

#JHove2 requires you to be in it's install dir when invoking it. Bad bug, this is our fix

pushd $HOME/tools/jhove2-2.0.0 > /dev/null
./jhove2.sh $@
popd > /dev/null