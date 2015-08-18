# First create an alias for Avro tools.
#
# Make sure env var $AVRO_TOOLS is set
# export AVRO_TOOLS="${AVRO_TOOLS_HOME}/avro-tools-1.7.7.jar"
#
# Run from this directory

if [[ $AVRO_TOOLS = "" ]]
then
  echo '$AVRO_TOOLS variable is not defined, exit.'
  exit 1
else
  echo '$AVRO_TOOLS is defined:' $AVRO_TOOLS
fi

# Generate the protocol description file
java -jar $AVRO_TOOLS idl snpEffAnnotations.avdl snpEffAnnotations.avpr

# Generate the protocol classes
java -jar $AVRO_TOOLS compile protocol snpEffAnnotations.avpr ./../../java/

# Remove the bdgenomics generated classes, we don't need those because we specify the dependency on bdg-formats in SBT.
rm -rf ./../../java/org/bdgenomics/

# clean up intermediate file
rm *.avpr