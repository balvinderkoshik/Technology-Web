################################################################################
#                               General Details                                #
################################################################################
#                                                                              #
# Name                                                                         #
#     : Batch ID generator                                                     #
# File                                                                         #
#     : batch_id_generator.sh                                                  #
#                                                                              #
# Description                                                                  #
#     :                                                                        #
#                                                                              #
#                                                                              #
#                                                                              #
# Author                                                                       #
#     : abhijit                                                 #
#                                                                              #
################################################################################
#                           Module Environment Setup                           #
################################################################################

#Find the script file home
pushd . > /dev/null
SCRIPT_HOME="${BASH_SOURCE[0]}";
while([ -h "${SCRIPT_HOME}" ]); do
    cd "`dirname "${SCRIPT_HOME}"`"
    SCRIPT_HOME="$(readlink "`basename "${SCRIPT_HOME}"`")";
done
cd "`dirname "${SCRIPT_HOME}"`" > /dev/null
SCRIPT_HOME="`pwd`";
popd  > /dev/null

#
# Storing the old modiles_home value and restoring it
# at the ed of the script so that the next scripts being
# called would not be pointing to shared modules
OLD_MODULES_HOME=${MODULES_HOME}

#set all parent home environment variables
MODULE_HOME=`dirname ${SCRIPT_HOME}`
MODULES_HOME=`dirname ${MODULE_HOME}`
PROJECT_HOME=`dirname ${MODULES_HOME}`
PROJECTS_HOME=`dirname ${PROJECT_HOME}`
SUBJECT_AREA_HOME=`dirname ${PROJECTS_HOME}`
SUBJECT_AREAS_HOME=`dirname ${SUBJECT_AREA_HOME}`
HOME=`dirname ${SUBJECT_AREAS_HOME}`

#Loads user namespace properties file. User may override namespace properties
#in his home folder in that case that file should be loaded after default name
#space properties file is loaded so that user is given chance to create his own 
#namespace.
#NOTE: This parameter is also used in the functions.sh file 
USER_NAMESPACE_PROPERTIES_FILE='~/.nsrc'

#Load all configurations files in order
. ${HOME}/etc/namespace.properties                                                             
if [ -f "$USER_NAMESPACE_PROPERTIES_FILE" ];                                                             
then
. ${USER_NAMESPACE_PROPERTIES_FILE}
fi
. ${HOME}/etc/default.env.properties                                                                     
. ${MODULE_HOME}/etc/module.env.properties


#load all utility functions
. ${HOME}/bin/functions.sh

################################################################################
#                           Common Environment Variables                       #
################################################################################
#get rid of existing LIBJARS environment variable. We dont want to unknown 
#files from LIBJARS

################################################################################
#                  Application Specific Environment Variables                  #
################################################################################

INPUT_PATH=$1
OUTPUT_PATH=$2
INPUT_DELIMITER=$3
SEQUENCE_NUM_PATH=$4

###
# checking if input exists. 
# fails if input doesn't exists
#
  hadoop fs -test -d "${INPUT_PATH}"
  fn_handle_exit_code \
    $? \
    "input path exists : ${INPUT_PATH}" \
    "input path doesn't exist : ${INPUT_PATH}"

###
# checking if output path exists
# fails if output exists
#
  hadoop fs -test -d "${OUTPUT_PATH}"
  output_path_exists=$?
  if [ "$output_path_exists" == "0" ]
  then
    exit_code=1
  else
    exit_code=0
  fi

  fn_handle_exit_code \
    $exit_code \
    "output path doesn't exist : ${OUPUT_PATH}"
    "output path exists already : ${OUPUT_PATH}"

###
# checking if sequence path exists
# fails if doesn't exists
#
  hadoop fs -test -d "${SEQUENCE_NUM_PATH}"
  fn_handle_exit_code \
    $? \
    "sequence number path exists : ${SEQUENCE_NUM_PATH}"
    "sequencer number path doesn't exist : ${SEQUENCE_NUM_PATH}"



fn_execute_mapreduce ${HOME}/repository/${SEQUENCER_JAR} \
  $INPUT_PATH \
  $OUTPUT_PATH \
  $INPUT_DELIMITER \
  $SEQUENCE_NUM_PATH


#
# Storing the old modiles_home value and restoring it
# at the ed of the script so that the next scripts being
# called would not be pointing to shared modules
MODULES_HOME=${OLD_MODULES_HOME}

################################################################################
#                                     End                                      #
################################################################################
