#!/usr/bin/env bash

# AVRO version = 1.7.7
#
# Make sure env var $AVRO_TOOLS is set, e.g.:
#   export AVRO_TOOLS="${AVRO_TOOLS_HOME}/avro-tools-1.7.7.jar"
#
# Run from this directory

if [[ $AVRO_TOOLS = "" ]]
then
  echo '$AVRO_TOOLS variable is not defined, exit.'
  exit 1
else
  echo '$AVRO_TOOLS is defined:' $AVRO_TOOLS
fi

# Clean the avro models package
echo '- cleaning target package'
rm -rf ./../../java/org/tmoerman/adam/fx/avro/

# Generate the protocol description file
echo '- converting IDL file to protocol file'
java -jar $AVRO_TOOLS idl adam-fx.avdl adam-fx.avpr

# Generate the protocol classes
echo '- compiling protocol file'
java -jar $AVRO_TOOLS compile -string protocol adam-fx.avpr ./../../java/

# Remove the bdgenomics generated classes, we don't need those because we specify the dependency on bdg-formats in SBT.
echo '- removing bdgenomics classes'
rm -rf ./../../java/org/bdgenomics/

# clean up intermediate file
echo '- cleaning intermediate files'
rm *.avpr

echo 'Done.'