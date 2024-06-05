#!/bin/sh
################################################################################
#                               General Details                                #
################################################################################
#                                                                              #
# Name                                                                         #
#     : deduplication                                                          #
# File                                                                         #
#     : module.sh                                                              #
#                                                                              #
# Description                                                                  #
#     :                                                                        #
#                                                                              #
#                                                                              #
#                                                                              #
# Author                                                                       #
#     : Hemanth Meka                                                           #
#                                                                              #
################################################################################
#                           Script Environment Setup                           #
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

#this generic module executes multiple module's code based on the child module
#name passed as an extra parameter to this module.sh from project.sh script
if [ $# -eq 0 ]
  then
    echo "Usage: <project home>/module.sh setup|transform <name of the child module>"
fi

child_module=$2

#Load all configurations files in order
. ${HOME}/etc/namespace.properties                                                             
if [ -f "$USER_NAMESPACE_PROPERTIES_FILE" ];                                                             
then
. ${USER_NAMESPACE_PROPERTIES_FILE}
fi
. ${HOME}/etc/default.env.properties                                                                     
. ${SUBJECT_AREA_HOME}/etc/subject-area.env.properties
. ${PROJECT_HOME}/etc/project.env.properties
. ${MODULE_HOME}/etc/module.env.properties
. ${MODULE_HOME}/etc/module.pig.properties
. ${MODULE_HOME}/etc/${child_module}.module.env.properties
#. ${MODULE_HOME}/etc/${child_module}.module.pig.properties
. ${MODULE_HOME}/etc/${child_module}.module.sqoop.properties
#load all utility functions
. ${HOME}/bin/functions.sh

################################################################################
#                                 Declaration                                  #
################################################################################


################################################################################
#                                  Initialise                                  #
################################################################################



################################################################################
#                                Implementation                                #
################################################################################


function fn_module_prepare(){

  step=$1

  case "${step}" in

    generate_batch_id)

      fn_generate_batch_id

    ;;

    something_else)

      fn_module_cleanup "${@:2}"

    ;;

    *)

      echo $"Usage: $0 {generate_batch_id|something_else}"

      exit 1

  esac


}

################################################################################
#                                     Main                                     #
################################################################################

fn_main "$@"
