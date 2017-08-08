#!/bin/sh

if [ $# -ne 1 ]
then
       DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
else
        DIR=$1

fi
cd $DIR

PROGRAM_DIR="$(dirname "$DIR")"
echo PROGRAM_DIR is $PROGRAM_DIR
PROGRAM="$(basename $DIR)"
echo PROGRAM is $PROGRAM

set -o nounset

./node_modules/forever/bin/forever stop $PROGRAM
