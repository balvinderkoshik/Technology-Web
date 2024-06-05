#!bin/bash
###############################################################################
#                               General Details                               #
###############################################################################
#                                                                             #
# Name                                                                        #
#     : functions.sh                                                          #
#                                                                             #
# Description                                                                 #
#     : This script consists of all the utility functions that are used by    #
#       module scripts. Modules shall not call and hadoop related commands    #
#       directly but instead write a wrapper function in this script with     #
#       proper handling of exit codes of the commands.                        #
#                                                                             #
# Author                                                                      #
#     : Manish Kumar                                                          #
#                                                                             #
# Note                                                                        #
#     : 1) Variables defined as final in the document must not be modified    #
#          by the module script code.                                         #
#       2) If function argument ends with * then it means that required       #
#          argument.                                                          #
#       3) If function argument ends with ? then it means that optional       #
#          argument.                                                          #
#                                                                             #
###############################################################################
#                     Global Environment Base Properties                      #
###############################################################################
###
# Integer representation of boolean true
# @Type  :  Integer
# @Final :  true
BOOLEAN_TRUE=1

###
# Integer representation of boolean false
# @Type  :  Integer
# @Final :  true
BOOLEAN_FALSE=0

###
# Integer representation of success exit code
# @Type  :  Integer
# @Final :  true
EXIT_CODE_SUCCESS=0


###
# Integer representation of fail exit code
# @Type  :  Integer
# @Final :  true
EXIT_CODE_FAIL=1

###
# Integer representation of exit code that will be returned when value for
# a required variable is not set
# @Type  :  Integer
# @Final :  true
EXIT_CODE_VARIABLE_NOT_SET=-10

###
# Holds current batch id.
# Do not modify this in the module scripts. BATCH_ID could be set by the
# workflow schedulers and passed into module scripts environment and can
# be accessed in this file.
# @Type   :  String
# @Format :  YYYYMMDDHHMMSS
# @Final  :  true
BATCH_ID="${BATCH_ID}"

###
# Holds current run date.
# Do not modify this in the module scripts. RUN_DATE could be set by the
# workflow schedulers and passed into module scripts environment and can
# be accessed in this file.
# @Type   :  String
# @Format :  YYYY-MM-DD
# @Final  :  true
RUN_DATE="${RUN_DATE}"

###
# Flat to check if the log directory for this module is setup or not.
# @Type  :  Boolean
# @Final :  flase
LOG_DIR_MODULE_SET="false"

###
# Log file name used to form log file names for
# the same module. Module log dir is set in the module's env properties file.
# @Type  :  String
# @Final :  true
LOG_FILE_NAME_MODULE=""

###
# Hive provides option to only pass single initialization file. To support
# overriding of hive variables at namespace, project, module level, a single
# file needs to be created out of the individual files provided at each level.
# This variable holds name of that file.
# @Type  :  Path
# @Final :  true
HIVE_INITIALISATION_FILE=""

###
# Path to repository directory which contains all the dependencies seperated
# by version. This allows multiple framework versions to co-exist across multiple
# projects or modules.
# @Type  :  Path
# @Final :  true
REPOSITORY_DIR="${HOME}/repository"

#List of Email Group
EMAIL_GROUP=expithadoopsupportteam@express.com,dmexpressoffshore@datametica.com,dmexpressops@datametica.com

################################################################################
#                             Function Definitions                             #
################################################################################

###
# Log any message
#
# Arguments:
#   message*
#     - string message
#
function fn_log(){
message=$1
echo ${message}

}


###
# Log info message
#
# Arguments:
#   message*
#     - string message
#
function fn_log_info(){

message=$1

fn_log "INFO ${message}"

}


###
# Log warning message
#
# Arguments:
#   message*
#     - string message
#
function fn_log_warn(){

message=$1

fn_log "WARN ${message}"

}


###
# Log error message
#
# Arguments:
#   message*
#     - string message
#
function fn_log_error(){

message=$1

fn_log "ERROR ${message}"

}


###
# Log given error message and exit the script with given exit code
#
# Arguments:
#   exit_code*
#     - exit code to be checked and exited with in case its not zero
#   failure_message*
#     - message to log if the exit code is non zero
#
function fn_exit_with_failure_message(){

exit_code=$1

failure_message=$3

fn_log_error "${failure_message}"

exit "${exit_code}"

}


###
# Check the exit code and then log the message for success or failure based on
# exit code. If fail_on_error flag value is non zero and the exit code is non
# zero then this process exits with that exit code.
#
# Arguments:
#   exit_code*
#     - exit code to be checked
#   success_message*
#     - message to log if the exit code is zero
#   failure_message*
#     - message to log if the exit code is non-zero
#   fail_on_error?
#     - Flag to decide, in case of failure of this operation, wheather to exit the process
#       with error code or just write error message and return.
#
function fn_handle_exit_code(){

exit_code=$1

success_message=$2

failure_message=$3

fail_on_error=$4

if [ -z ${fail_on_error} ]
then
fail_on_error=1
fi

if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
then

fn_log_error "${failure_message}"

if [ ${fail_on_error} -ne $EXIT_CODE_SUCCESS ]
then

exit "${exit_code}"

fi

else

fn_log_info "${success_message}"

fi

}

###
# Verify if the variable is not-set/empty or not. In case it is non-set or empty,
# exit with failure message.
#
# Arguments:
#   variable_name*
#     - name of the variable to be check for being not-set/empty
#   variable_value*
#     - value of the variable
#
function assert_variable_is_set(){

variable_name=$1

variable_value=$2

if [ "${variable_value}" == "" ]
then

exit_code=${EXIT_CODE_VARIABLE_NOT_SET}

failure_messages="Value for ${variable_name} variable is not set"

fn_exit_with_failure_message "${exit_code}" "${failure_messages}"

fi

}


###
# Delete local file
#
# Arguments:
#   file*
#     - path of the file to be deleted
#   fail_on_error?
#     - Flag to decide, in case of failure of this operation, wheather to exit the process
#       with error code or just write error message and return.
#
function fn_local_delete_file(){

file="$1"

fail_on_error="$2"

assert_variable_is_set "file" "${file}"

rm -f "${file}"

exit_code="$?"

success_message="Deleted file ${file}"

failure_message="Failed to delete file ${file}"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

###
# Create new local directory if it does not already exist
#
# Arguments:
#   directory*
#     - path of the directory to be created
#   fail_on_error?
#     - Flag to decide, in case of failure of this operation, wheather to exit the process
#       with error code or just write error message and return.
#
function fn_local_create_directory_if_not_exists(){

directory=$1

fail_on_error=$2

assert_variable_is_set "directory" "${directory}"

if [ ! -d "${directory}" ]
then

mkdir -p ${directory}

exit_code=$?

success_message="Created local directory ${directory}"

failure_message="Failed to create local directory ${directory}"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

else

fn_log_info "Local directory ${directory} already exists"

fi

}

###
# Copy files to HDFS folder
#
# Arguments:
#   file*
#     - path of the file or directory to be moved
#   to_hdfs_directory*
#     - path of the target HDFS directory
#   skip_tests?
#     - skip all the path checks. This is useful if this function is called in a loop.
#   fail_on_error?
#     - flag to decide, in case of failure of this operation, wheather to exit the process
#       with error code or just write error message and return.
#

function fn_copy_files_to_hdfs(){  # Need to handle failure conditions

file_or_folder=$1

to_hdfs_directory=$2

fail_on_error=$3

assert_variable_is_set "file_or_folder" "${file_or_folder}"
assert_variable_is_set "to_hdfs_directory" "${to_hdfs_directory}"
hdfs dfs -copyFromLocal ${file_or_folder} "${to_hdfs_directory}"
exit_code=$?
success_message="Successfully copied files from ${file_or_folder} to HDFS directory ${to_hdfs_directory}"
failure_message="Failed to copy files from ${file_or_folder} to HDFS directory ${to_hdfs_directory}"
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"
}




###
# Delete hadoop directory if it already exists
#
# Arguments:
#   directory*
#     - path of the directory to be created
#   fail_on_error?
#     - Flag to decide, in case of failure of this operation, wheather to exit the process
#       with error code or just write error message and return.
#
function fn_hadoop_delete_directory_if_exists(){

directory=$1

fail_on_error=$2

assert_variable_is_set "directory" "${directory}"

hdfs dfs -test -e "${directory}"

exit_code=$?

if [ ${exit_code} == $EXIT_CODE_SUCCESS ]
then

hdfs dfs -rm -r "${directory}"

exit_code=$?

success_message="Deleted hadoop directory ${directory}"

failure_message="Failed to delete hadoop directory ${directory}"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

else

fn_log_info "Hadoop directory ${directory} does not exist"

fi

}

###
# Create hadoop directory if it does not already exist
#
# Arguments:
#   directory*
#     - path of the directory to be created
#   fail_on_error?
#     - Flag to decide, in case of failure of this operation, wheather to exit the process
#       with error code or just write error message and return.
#
function fn_hadoop_create_directory_if_not_exists(){

directory=$1

fail_on_error=$2

assert_variable_is_set "directory" "${directory}"

hdfs dfs -test -e "${directory}"

exit_code=$?

if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
then

hdfs dfs -mkdir -p "${directory}"

exit_code=$?

success_message="Created hadoop directory ${directory}"

failure_message="Failed to create hadoop directory ${directory}"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

else

fn_log_info "Hadoop directory ${directory} already exists"

fi
}



###
# Moves a hadoop file or folder to another hadoop folder
#
# Arguments:
#   file_or_directory*
#     - path of the file or directory to be moved
#   to_directory*
#     - path of the target directory
#   skip_tests?
#     - skip all the path checks. This is useful if this function is called in a loop.
#   fail_on_error?
#     - flag to decide, in case of failure of this operation, wheather to exit the process
#       with error code or just write error message and return.
#
function fn_hadoop_move_file_or_directory(){

file_or_directory=$1

to_directory=$2

skip_tests=$3

fail_on_error=$4

if [ "${skip_tests}" != "${BOOLEAN_TRUE}" ]
then

assert_variable_is_set "file_or_directory" "${file_or_directory}"

assert_variable_is_set "to_directory" "${to_directory}"

hdfs dfs -test -e "${file_or_directory}"

exit_code=$?

success_message="Source hadoop file/directory ${file_or_directory} exists"

failure_message="Source hadoop file/directory does not exist"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

fn_hadoop_create_directory_if_not_exists "${to_directory}" "${fail_on_error}"

fi

hadoop fs -mv "${file_or_directory}" "${to_directory}"

exit_code=$?

success_message="Successfully moved file or directory ${file_or_directory} to directory ${to_directory}"

failure_message="Failed to move file or directory ${file_or_directory} to directory ${to_directory}"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}





###
# Generate new batch id.
# This function implementation is based on generating timestamp as batch id.
# But it may be used to simple test purposes. In case of enterprise scenarios,
# entrerprise workflow scheduler should be used which has capability to run
# multiple batches in parallel. This implementation stores the newly generated
# atch id into a file named as current into the directory configured using
# BATCH_ID_DIR environment variable. For this first time this file wont be
# present hence it will be create. Every next time the contents of the current
# file are moved into a file named after the batch id store into current file.
#
function fn_generate_batch_id(){

fn_log_info "Generating new batch id"

fail_on_error=$BOOLEAN_TRUE
dont_fail_on_error=$BOOLEAN_FALSE

assert_variable_is_set "BATCH_ID_DIR" "${BATCH_ID_DIR}"

fn_local_create_directory_if_not_exists "${BATCH_ID_DIR}" "${fail_on_error}"

new_batch_id=`date +"%Y%m%d%H%M%S"`

current_batch_id_file="${BATCH_ID_DIR}/current"
previous_batch_id_file="${BATCH_ID_DIR}/previous"

if [ ! -f "${current_batch_id_file}" ]
then

fn_log_info "Current batch id file ${current_batch_id_file} does not exists."

else

current_batch_id=`cat ${current_batch_id_file}`

fn_local_delete_file "${previous_batch_id_file}" "${dont_fail_on_error}"

cp "${current_batch_id_file}" "${previous_batch_id_file}"

mv "${current_batch_id_file}" "${BATCH_ID_DIR}/${current_batch_id}"

exit_code=$?

success_message="Moved current batch id file to ${BATCH_ID_DIR}/${current_batch_id}"

failure_messages="Failed to move current file to ${BATCH_ID_DIR}/${current_batch_id} file"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_messages}" "${dont_fail_on_error}"

fi

touch "${current_batch_id_file}"

echo "${new_batch_id}" > "${current_batch_id_file}"

exit_code=$?

fail_on_error=1

success_message="Generated new batch id is ${new_batch_id} and wrote it to ${current_batch_id_file} file "

failure_messages="Failed to write new batch id ${new_batch_id} to ${current_batch_id_file} file"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_messages}" "${fail_on_error}"

}



###
# Set the global BATCH_ID with current batch id if it not already set
#
function fn_get_current_batch_id(){

if [ -z "${BATCH_ID}" ];
then

current_batch_id_file="${BATCH_ID_DIR}/current"

BATCH_ID=`cat ${current_batch_id_file}`

fi

assert_variable_is_set "BATCH_ID" "${BATCH_ID}"

}


###
# Set the global PREVIOUS_BATCH_ID with previous batch id if it not already set
#
function fn_get_previous_batch_id(){

if [ -z "${PREVIOUS_BATCH_ID}" ];
then

previous_batch_id_file="${BATCH_ID_DIR}/previous"

PREVIOUS_BATCH_ID=`cat ${previous_batch_id_file}`

fi

assert_variable_is_set "PREVIOUS_BATCH_ID" "${PREVIOUS_BATCH_ID}"

}


###
# Generate new run date.
# This function implementation is based on generating date as run date.
# But it may be used to simple test purposes. In case of enterprise scenarios,
# entrerprise workflow scheduler should be used which has capability to run
# multiple run dates in parallel. This implementation stores the newly generated
# run date into a file named as current into the directory configured using
# RUN_DATE_DIR environment variable. For this first time this file won't be
# present hence it will be create. Every next time the contents of the current
# file are moved into a file named after the run date store into current file.
#
function fn_generate_run_date(){

fn_log_info "Generating new run date"

fail_on_error=$BOOLEAN_TRUE
dont_fail_on_error=$BOOLEAN_FALSE

assert_variable_is_set "RUN_DATE_DIR" "${RUN_DATE_DIR}"

fn_local_create_directory_if_not_exists "${RUN_DATE_DIR}" "${fail_on_error}"

new_run_date=`date +"%Y-%m-%d"`

current_run_date_file="${RUN_DATE_DIR}/current"
previous_run_date_file="${RUN_DATE_DIR}/previous"

if [ ! -f "${current_run_date_file}" ]
then

fn_log_info "Current run date file ${current_run_date_file} does not exists."

else

current_run_date=`cat ${current_run_date_file}`

fn_local_delete_file "${previous_run_date_file}" "${dont_fail_on_error}"

cp "${current_run_date_file}" "${previous_run_date_file}"

mv "${current_run_date_file}" "${RUN_DATE_DIR}/${current_run_date}"

exit_code=$?

success_message="Moved current run date file to ${RUN_DATE_DIR}/${current_run_date}"

failure_messages="Failed to move current file to ${RUN_DATE_DIR}/${current_run_date} file"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_messages}" "${dont_fail_on_error}"

fi

touch "${current_run_date_file}"

echo "${new_run_date}" > "${current_run_date_file}"

exit_code=$?

fail_on_error=1

success_message="Generated new run date is ${new_run_date} and wrote it to ${current_run_date_file} file "

failure_messages="Failed to write new run date ${new_run_date} to ${current_run_date_file} file"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_messages}" "${fail_on_error}"

}



###
# Set the global RUN_DATE with current run date if it not already set
#
function fn_get_current_run_date(){

if [ -z "${RUN_DATE}" ];
then

current_run_date_file="${RUN_DATE_DIR}/current"

RUN_DATE=`cat ${current_run_date_file}`

fi

assert_variable_is_set "RUN_DATE" "${RUN_DATE}"

}


###
# Set the global PREVIOUS_RUN_DATE with previous run date if it not already set
#
function fn_get_previous_run_date(){

if [ -z "${PREVIOUS_RUN_DATE}" ];
then

previous_run_date_file="${RUN_DATE_DIR}/previous"

PREVIOUS_RUN_DATE=`cat ${previous_run_date_file}`

fi

assert_variable_is_set "PREVIOUS_RUN_DATE" "${PREVIOUS_RUN_DATE}"

}


function fn_email_status(){
fn_status=$1
entity_name=$2
message=$3
log_file_path=$4
date=$(date)
if [ "${fn_status}" = 0 ]
then
echo "Sending Success Email. "
echo ${message} | mail -a "${log_file_path}" -s "Success Report For ${entity_name} on ${date}" -r "Express_NiFi_Alerts@express.com" "${alert_mailer_id}"
else
echo "Sending Failure Email."
echo ${message} | mail -a "${log_file_path}" -s "Failure Report For ${entity_name} on ${date}" -r "Express_NiFi_Alerts@express.com" "${alert_mailer_id}"
fi
exit_code_mail=$?

fail_on_error=$BOOLEAN_TRUE

success_message="Successfully sent email."

failure_message="Failed to send email."

fn_handle_exit_code "${exit_code_mail}" "${success_message}" "${failure_message}" "${fail_on_error}"
}


###
# Set the global LOG_FILE_NAME_MODULE variable if it is not already set with current batch id
#
function fn_get_module_log_dir_and_file(){
if [ "${LOG_DIR_MODULE_SET}" != "true" ]
then

fn_get_current_batch_id

assert_variable_is_set "BATCH_ID" "${BATCH_ID}"

assert_variable_is_set "LOG_DIR_MODULE" "${LOG_DIR_MODULE}"

TEMP_LOG_DIR_MODULE="${LOG_DIR_MODULE}/batch-${BATCH_ID}"

if [ ! -d "${TEMP_LOG_DIR_MODULE}" ]
then

mkdir -p ${TEMP_LOG_DIR_MODULE}

exit_code=$?

fail_on_error=${BOOLEAN_TRUE}

success_message="Created log directory ${TEMP_LOG_DIR_MODULE}"

failure_message="Failed to create log directory ${LOG_DIR_MODULE}"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

fi

log_file_name=`date +"%Y%d%m%H%M%S"`

LOG_FILE_NAME_MODULE="${log_file_name}.log"

LOG_DIR_MODULE="${TEMP_LOG_DIR_MODULE}"

LOG_DIR_MODULE_SET="true"

fi

}

###
# Execute NIFI Processor Group
#
# Arguments
# NIFI_URL*
#  - NIFI URL
# Process_group _d*
#  - Triggering Process group id i Nifi
# File*
#  - Name of the file for which processor has to trigger

function fn_execute_start_processor(){
attempt_num=1

CURL_EXEC_TOKEN=`echo "curl -k '${NIFI_ACCESS_TOKEN}' -v --data \"username=$username&password=$password\""`

CURL_TK=`eval "$CURL_EXEC_TOKEN"`
echo "curl_tk= ${CURL_TK}"

CURL_EXEC=`echo "curl -k '${NIFI_URL}/${PROCESS_GROUP_ID}' -X PUT -H 'Accept-Encoding: gzip, deflate, sdch, br' -H 'Content-Type: application/json' -H 'Accept: application/json, text/javascript, */*; q=0.01' --data-binary '{\"id\":\"$PROCESS_GROUP_ID\",\"state\":\"RUNNING\"}' --compressed -H \"Authorization: Bearer $CURL_TK\""`
echo "curl_exec= ${CURL_EXEC}"

eval "$CURL_EXEC"

if [ $? -eq 0 ]
then
echo "[INFO]:NIFI flow  is executing"
else
until eval "$CURL_EXEC"
do
if (( attempt_num == 3 ))
then
echo "Attempt $attempt_num failed to start NiFi Flow and there are no more attempts left!"
fn_email_status "1" "NiFi Workflow" "Failed to submit cURL Request to start NiFi Workflow." "${NIFI_LOG_FILE_PATH}/${LOG_FILE_NAME_MODULE}"
exit 1
else
echo "Attempt $attempt_num failed to start NiFi Flow! Trying again in $attempt_num seconds..."
sleep $(( attempt_num++ ))
fi
done
fi
}


function fn_execute_stop_processor(){
attempt_num=1

CURL_EXEC_TOKEN_S=`echo "curl -k '${NIFI_ACCESS_TOKEN}' -v --data \"username=$username&password=$password\""`

CURL_TK_S=`eval "$CURL_EXEC_TOKEN"`

CURL_EXEC_S=`echo "curl -k '${NIFI_URL}/${PROCESS_GROUP_ID}' -X PUT -H 'Accept-Encoding: gzip, deflate, sdch, br' -H 'Content-Type: application/json' -H 'Accept: application/json, text/javascript, */*; q=0.01' --data-binary '{\"id\":\"$PROCESS_GROUP_ID\",\"state\":\"STOPPED\"}' --compressed -H \"Authorization: Bearer $CURL_TK_S\""`
eval "$CURL_EXEC_S"

if [ $? -eq 0 ]
then
echo "[INFO]:NIFI flow stopped"
else
until eval "$CURL_EXEC_S"
do
if (( attempt_num == 3 ))
then
echo "Attempt $attempt_num failed to stop NiFi Workflow and there are no more attempts left!"
fn_email_status "1" "NiFi Workflow" "Failed to submit cURL Request to stop NiFi Workflow." "${NIFI_LOG_FILE_PATH}/${LOG_FILE_NAME_MODULE}"
echo "STOP THE PROCESS GROUP MANUALLY"
exit 1
else
echo "Attempt $attempt_num failed to stop NiFi Workflow for ${File}! Trying again in $attempt_num seconds..."
sleep $(( attempt_num++ ))
fi
done
fi
}

###
# Execute File Integrity Check Funtion
#
# Arguments
# FILE_IN_HDFS_LOC*
#  - Incoming File HDFS location
# CTRL_DB*
#  - Control table Connection information
# FILE_NAME_PATTERN*
#  - Pattern of the file for which processor has to trigger

fn_execute_ingestion_integrity_check(){
ING_FILE_NAMES=`hadoop fs -ls ${FILE_IN_HDFS_LOC}/${FILE_NAME_PATTERN}/${RUN_DT} | grep -i ${FILE_NAME_PATTERN} |awk -F'/' '{print $NF}'`
if [ $? -eq 0 ]
then
echo "[INFO]:Successfully listed all files to process."
else
echo "[ERROR]:Failed to list all files to process."
exit 1
fi

for i in ${ING_FILE_NAMES}
do
CHECKSUM_VAL=`hadoop fs -checksum ${FILE_IN_HDFS_LOC}/${FILE_NAME_PATTERN}/${RUN_DT}/${i} |awk '{print $NF}'`
if [ $? -eq 0 ]
then
echo "[INFO]:Checksum Value fo File ${FILE_IN_HDFS_LOC}/${RUN_DT}/${i}: ${CHECKSUM_VAL} "
else
echo "[ERROR]:Failed to calculate the Checksum value for the file ${FILE_IN_HDFS_LOC}/${RUN_DT}/${i} "
exit 1
fi
# Check for Checksum value in the Control table
CHECK_DB=`sqlplus -s ${ORA_CTRL_DB_USERNAME}/${ORA_CTRL_DB_PASSWORD}@${ORA_CTRL_DB_HOST}:1521/${ORA_CTRL_DB_SID} <<EOF
set pagesize 0
set feedback off
set verify off
set heading off
select count(*) from ${ORA_AUDIT_CTRL_TBL_NAME} where File_MD5_checksum_value='${CHECKSUM_VAL}';
EOF`
if [ $? -eq 0 ]
then
echo "[INFO]:Successfully queried Control Table. Retrived Value : ${CHECK_DB} "
else
echo "[ERROR]:Failed to query Control Table. "
exit 1
fi

# Routing Ingested file based on the DB Return Value

if [ ${CHECK_DB} == '0' ]
then
echo "[INFO]: ${FILE_IN_HDFS_LOC}/${FILE_NAME_PATTERN}/${RUN_DT}/${i} is a new file."
echo "[INFO]: Considering the same for further processing."
echo "[INFO]: Inserting a new record in the Audit Control table for the file ${FILE_IN_HDFS_LOC}/${FILE_NAME_PATTERN}/${RUN_DT}/${i}"

sqlplus -s ${ORA_CTRL_DB_USERNAME}/${ORA_CTRL_DB_PASSWORD}@${ORA_CTRL_DB_HOST}:1521/${ORA_CTRL_DB_SID} <<EOF
set pagesize 0
set feedback off
set verify off
set heading off
UPDATE ${ORA_AUDIT_CTRL_TBL_NAME} SET File_MD5_checksum_value='${CHECKSUM_VAL}' WHERE Filename_original='${i}';
EOF

if [ $? -eq 0 ]
then
echo "[INFO]:Successfully Inserted record in Audit Control Table."
else
echo "[ERROR]:Failed to insert record in Audit COntrol Table."
exit 1
fi
else
echo "[WARNING]: MD5 Value for File ${FILE_IN_HDFS_LOC}/${FILE_NAME_PATTERN}/${RUN_DT}/${i} matches with one of the already processed file."
echo "[WARNING]: Moving this file into the HDFS Error Directory."

# Create the Error HDFS location if does not exists
hadoop fs -mkdir -p ${FILE_ERR_HDFS_LOC}/${FILE_NAME_PATTERN}/${RUN_DT}/

# Move File from Incoming Dir to Error Dir
hadoop fs -mv ${FILE_IN_HDFS_LOC}/${FILE_NAME_PATTERN}/${RUN_DT}/${i} ${FILE_ERR_HDFS_LOC}/${FILE_NAME_PATTERN}/${RUN_DT}/
if [ $? -eq 0 ]
then
echo "[INFO]:Successfully moved File ${FILE_IN_HDFS_LOC}/${FILE_NAME_PATTERN}/${RUN_DT}/${i} from Incoming to Error Location."
fn_email_status "1" "${FILE_NAME_PATTERN}" "File Moved from INCOMING Layer to ERROR Layer in HDFS as captured MD5 Value already exist in Audit COntrol Table." "${INTEGRITY_LOG_FILE_NAME}"
else
echo "[ERROR]:Failed to move File ${FILE_IN_HDFS_LOC}/${FILE_NAME_PATTERN}/${RUN_DT}/${i} from Incoming to Error Location."
exit 1
fi
fi
done
}

###
# Creates directory structure for the hive table
#
# Arguments:
#   data_layer_dir*
#     - data layer where this table has to be created
#   hive_table_name* -
#     - name of the hive table to be used to create folder
#   create_landing_partition?
#     - wheater to create landing partition or not landing partition is only applicable for
#     - incoming or security layers as input marks needs to be supported.
#
function fn_module_create_hive_table_data_layer_directory_structure(){

data_layer_dir="$1"

hive_table_name="$2"

create_landing_partition="$3"

assert_variable_is_set "data_layer_dir" "${data_layer_dir}"

assert_variable_is_set "hive_table_name" "${hive_table_name}"

fn_log_info "Creating module table ${hive_table_name} in data layer ${data_layer_dir}"

fail_on_error="${BOOLEAN_TRUE}"

hive_table_data_layer_dir="${data_layer_dir}/${hive_table_name}"

fn_hadoop_create_directory_if_not_exists "${hive_table_data_layer_dir}" "${fail_on_error}"

fn_log_info "Successfully created module table ${hive_table_name} directory in data layer ${data_layer_dir}"


if [ "${create_landing_partition}" == "${BOOLEAN_TRUE}" ];
then

assert_variable_is_set "LANDING_PARTITION_BATCH_ID" "${LANDING_PARTITION_BATCH_ID}"

hive_table_data_layer_landing_partition_dir="${hive_table_data_layer_dir}/batch_id=${LANDING_PARTITION_BATCH_ID}"

fn_hadoop_create_directory_if_not_exists "${hive_table_data_layer_landing_partition_dir}" "${fail_on_error}"

fn_log_info "Successfully created module table ${hive_table_name} directory in data layer ${data_layer_dir} with landing partition id"

fi
}

##
# Mark current batch input
#
# Arguments:
#   module_landing_partition_dir
#     - path to the landing partition directory
#   module_marked_batch_id_dir
#     - path to the current batch id directory
#
function fn_mark_batch_input(){

module_landing_partition_dir=$1

module_marked_batch_id_dir=$2

assert_variable_is_set "module_landing_partition_dir" "${module_landing_partition_dir}"

assert_variable_is_set "module_marked_batch_id_dir" "${module_marked_batch_id_dir}"

fail_on_error="${BOOLEAN_TRUE}"

fn_hadoop_create_directory_if_not_exists "${module_marked_batch_id_dir}" "${fail_on_error}"

module_marked_batch_id_dir_complete_file="${module_marked_batch_id_dir}/_COMPLETE"

hadoop fs -test -e "${module_marked_batch_id_dir_complete_file}"

exit_code=$?

if [ ${exit_code} != $EXIT_CODE_SUCCESS ]
then

#if input path ends with forward slash
if [[ "${module_landing_partition_dir}" == */ ]]
then

calculated_hidden_file_or_folder_prefix="${module_landing_partition_dir}${HIDDEN_FILE_OR_DIRECTORY_PREFIX}"

else

#add forward slash
calculated_hidden_file_or_folder_prefix="${module_landing_partition_dir}/${HIDDEN_FILE_OR_DIRECTORY_PREFIX}"

fi

#find all the files from the landing partition directory
list_of_all_files=$(hadoop fs -ls ${module_landing_partition_dir} | awk '{print $8}')

skip_tests="${BOOLEAN_FALSE}"

atleast_one_file_if_moved="${BOOLEAN_FALSE}"

for each_file in ${list_of_all_files}
do

file_name=${each_file}

if [ "${file_name}" != "" ]
then

#If the file does not start with underscore character which is used for hidden files
if [ "${file_name:0:${#calculated_hidden_file_or_folder_prefix}}" != "${calculated_hidden_file_or_folder_prefix}"  ]
then

fail_on_error="${BOOLEAN_TRUE}"

fn_hadoop_move_file_or_directory  "${file_name}" "${module_marked_batch_id_dir}" "${skip_tests}" "${fail_on_error}"

skip_tests="${BOOLEAN_TRUE}"

atleast_one_file_if_moved="${BOOLEAN_TRUE}"

fi
fi
done

if [ "${atleast_one_file_if_moved}" == "${BOOLEAN_TRUE}" ]
then
#all the files are successfullt moved from landing partition to current batch id partition.
#mark the success by creating _COMPLETE file
hadoop fs -touchz "${module_marked_batch_id_dir_complete_file}"

exit_code=$?

fail_on_error=$BOOLEAN_TRUE

success_message="Successfully created ${module_marked_batch_id_dir_complete_file} file"

failure_message="Failed to create ${module_marked_batch_id_dir_complete_file} file"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"
fi

else

fn_log_info "Input for this batch is already marked at directory ${module_marked_batch_id_dir}. Skipping marking again."

fi

}


##
# Generate hive initialization script
#
#
function fn_generate_hive_initialization_file(){

fail_on_error="${BOOLEAN_TRUE}"

fn_local_delete_file "${HIVE_INITIALISATION_FILE}" "${fail_on_error}"

for line in `grep -v '^#' ${HOME}/etc/namespace.properties`
do

echo "set ${line};" >> "${HIVE_INITIALISATION_FILE}"

done

if [ -f "${USER_NAMESPACE_PROPERTIES_FILE}" ];
then

for line in `grep -v '^#' ${USER_NAMESPACE_PROPERTIES_FILE}`
do

echo "set ${line};" >> "${HIVE_INITIALISATION_FILE}"

done

fi

echo "" >> "${HIVE_INITIALISATION_FILE}"

cat "${HOME}/etc/default.hive.properties" >> "${HIVE_INITIALISATION_FILE}"

echo "" >> "${HIVE_INITIALISATION_FILE}"

cat "${SUBJECT_AREA_HOME}/etc/subject-area.hive.properties" >> "${HIVE_INITIALISATION_FILE}"

echo "" >> "${HIVE_INITIALISATION_FILE}"

cat "${PROJECT_HOME}/etc/project.hive.properties" >> "${HIVE_INITIALISATION_FILE}"

echo "" >> "${HIVE_INITIALISATION_FILE}"

cat "${MODULE_HOME}/etc/module.hive.properties" >> "${HIVE_INITIALISATION_FILE}"

echo "" >> "${HIVE_INITIALISATION_FILE}"

}


###
# Creates hive database
#
# Arguments:
#   hive_database_name
#     - name of the hive database
#   hive_database_location
#     - location to be used for the database
#
function fn_create_hive_database(){

hive_database_name="$1"

hive_database_location="$2"

assert_variable_is_set "hive_database_name" "${hive_database_name}"

assert_variable_is_set "hive_database_location" "${hive_database_location}"

hive_database_name_with_prefix="${HIVE_DATABASE_PREFIX}${hive_database_name}"

hive -e "CREATE DATABASE IF NOT EXISTS ${hive_database_name_with_prefix} LOCATION '${hive_database_location}'"

exit_code=$?

fail_on_error="$BOOLEAN_TRUE"

success_message="Successfully created hive database ${hive_database_name_with_prefix} with location ${hive_database_location}"

failure_message="Failed to create hive database ${hive_database_name_with_prefix} with location ${hive_database_location}"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

##
# Execute hive script
# Arguments:
#   pass any arguments that you want to pass to hive
#   command
#
#
function fn_create_hive_table(){

fn_get_current_batch_id

fn_get_module_log_dir_and_file

hive_database="$1"

data_layer_dir="$2"

hive_table_name="$3"

hive_ddl_file="$4"

hive_table_location="${data_layer_dir}/${hive_table_name}"

assert_variable_is_set "hive_database" "${hive_database}"

assert_variable_is_set "hive_ddl_file" "${hive_ddl_file}"

module_type="hive"

module_log_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}"

mobule_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"

module_err_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.err"

HIVE_INITIALISATION_FILE="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.rc"

fn_generate_hive_initialization_file

fn_log_info "Log file : ${module_log_file}"

fn_log_info "Out file : ${mobule_out_file}"

fn_log_info "Err file : ${module_err_file}"

hive -d batch.id="${BATCH_ID}" \
-hiveconf database="${HIVE_DATABASE_PREFIX}${hive_database}" \
-hiveconf namespace="${HIVE_TABLE_PREFIX}" \
-hiveconf location="${hive_table_location}" \
-hiveconf table_name="${hive_table_name}" \
-hiveconf ${UNIQUE_PROPERTY_PREFIX}.batch.id="${BATCH_ID}" \
-hiveconf ${UNIQUE_PROPERTY_PREFIX}.subject.area.name="${SUBJECT_AREA_NAME}" \
-hiveconf ${UNIQUE_PROPERTY_PREFIX}.project.name="${PROJECT_NAME}" \
-hiveconf ${UNIQUE_PROPERTY_PREFIX}.module.name="${MODULE_NAME}" \
-hiveconf hive.querylog.location="${LOG_DIR_MODULE}/${module_log_file}" \
-i "${HIVE_INITIALISATION_FILE}" \
-f "${hive_ddl_file}" $@ 1>> "${mobule_out_file}" 2>> "${module_err_file}"

exit_code=$?

fail_on_error=$BOOLEAN_TRUE

success_message="Successfully created hive table ${hive_ddl_file} hive script"

failure_message="Failed to create hive table ${hive_ddl_file} hive script"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"
}


##
# Execute hive script
# Arguments:
#   pass any arguments that you want to pass to hive
#   command
#
#
function fn_add_batch_id_partition_to_hive_table(){

hive_database="$1"

hive_table="$2"

partition_batch_id="$3"

assert_variable_is_set "hive_database" "${hive_database}"

assert_variable_is_set "hive_table" "${hive_table}"

assert_variable_is_set "partition_batch_id" "${partition_batch_id}"

fn_get_current_batch_id

fn_get_module_log_dir_and_file

module_type="hive"

module_log_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}"

mobule_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"

module_err_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.err"

HIVE_INITIALISATION_FILE="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.rc"

fn_generate_hive_initialization_file

fn_log_info "Log file : ${module_log_file}"

fn_log_info "Out file : ${mobule_out_file}"

fn_log_info "Err file : ${module_err_file}"

hive_database_name_with_prefix="${HIVE_DATABASE_PREFIX}${hive_database}"


hive -d batch.id="${partition_batch_id}" \
-hiveconf ${UNIQUE_PROPERTY_PREFIX}.batch.id="${partition_batch_id}" \
-hiveconf ${UNIQUE_PROPERTY_PREFIX}.subject.area.name="${SUBJECT_AREA_NAME}" \
-hiveconf ${UNIQUE_PROPERTY_PREFIX}.project.name="${PROJECT_NAME}" \
-hiveconf ${UNIQUE_PROPERTY_PREFIX}.module.name="${MODULE_NAME}" \
-hiveconf hive.querylog.location="${LOG_DIR_MODULE}/${module_log_file}" \
-i "${HIVE_INITIALISATION_FILE}" \
-e  "use ${hive_database_name_with_prefix}; ALTER TABLE ${HIVE_TABLE_PREFIX}${hive_table} ADD PARTITION (batch_id = '${partition_batch_id}');" $@ 1>> "${mobule_out_file}" 2>> "${module_err_file}"

exit_code=$?

fail_on_error=$BOOLEAN_TRUE

success_message="Successfully added parition ${partition_batch_id} to hive table ${hive_table}"

failure_message="Failed to add parition ${partition_batch_id} to hive table ${hive_table}"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"
}

function fn_drop_batch_id_partition_to_hive_table(){

hive_database="$1"

hive_table="$2"

partition_batch_id="$3"

assert_variable_is_set "hive_database" "${hive_database}"

assert_variable_is_set "hive_table" "${hive_table}"

assert_variable_is_set "partition_batch_id" "${partition_batch_id}"

fn_get_current_batch_id

fn_get_module_log_dir_and_file

module_type="hive"

module_log_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}"

mobule_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"

module_err_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.err"

HIVE_INITIALISATION_FILE="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.rc"

fn_generate_hive_initialization_file

fn_log_info "Log file : ${module_log_file}"

fn_log_info "Out file : ${mobule_out_file}"

fn_log_info "Err file : ${module_err_file}"

hive_database_name_with_prefix="${HIVE_DATABASE_PREFIX}${hive_database}"


hive -d batch.id="${partition_batch_id}" \
-hiveconf ${UNIQUE_PROPERTY_PREFIX}.batch.id="${partition_batch_id}" \
-hiveconf ${UNIQUE_PROPERTY_PREFIX}.subject.area.name="${SUBJECT_AREA_NAME}" \
-hiveconf ${UNIQUE_PROPERTY_PREFIX}.project.name="${PROJECT_NAME}" \
-hiveconf ${UNIQUE_PROPERTY_PREFIX}.module.name="${MODULE_NAME}" \
-hiveconf hive.querylog.location="${LOG_DIR_MODULE}/${module_log_file}" \
-i "${HIVE_INITIALISATION_FILE}" \
-e  "use ${hive_database_name_with_prefix}; ALTER TABLE ${HIVE_TABLE_PREFIX}${hive_table} DROP PARTITION (batch_id = '${partition_batch_id}');" $@ 1>> "${mobule_out_file}" 2>> "${module_err_file}"

exit_code=$?

fail_on_error=$BOOLEAN_TRUE

success_message="Successfully dropped parition ${partition_batch_id} to hive table ${hive_table}"

failure_message="Failed to drop parition ${partition_batch_id} to hive table ${hive_table}"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"
}

##
# Execute hive script
# Arguments:
#   pass any arguments that you want to pass to hive
#   command
#
#

function fn_add_two_partitions_to_hive_table(){

hive_database="$1"

hive_table="$2"

first_partition="$3"

second_partition="$4"

assert_variable_is_set "hive_database" "${hive_database}"

assert_variable_is_set "hive_table" "${hive_table}"

assert_variable_is_set "first_partition" "${first_partition}"

assert_variable_is_set "second_partition" "${second_partition}"

fn_get_current_batch_id

fn_get_module_log_dir_and_file

module_type="hive"

module_log_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}"

mobule_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"

module_err_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.err"

HIVE_INITIALISATION_FILE="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.rc"

fn_generate_hive_initialization_file

fn_log_info "Log file : ${module_log_file}"

fn_log_info "Out file : ${mobule_out_file}"

fn_log_info "Err file : ${module_err_file}"

hive_database_name_with_prefix="${HIVE_DATABASE_PREFIX}${hive_database}"

hive -d first.partition="${first_partition}" \
-d second.partition="${second_partition}" \
-hiveconf ${UNIQUE_PROPERTY_PREFIX}.first.partition="${first_partition}" \
-hiveconf ${UNIQUE_PROPERTY_PREFIX}.second.partition="${second_partition}" \
-hiveconf ${UNIQUE_PROPERTY_PREFIX}.subject.area.name="${SUBJECT_AREA_NAME}" \
-hiveconf ${UNIQUE_PROPERTY_PREFIX}.project.name="${PROJECT_NAME}" \
-hiveconf ${UNIQUE_PROPERTY_PREFIX}.module.name="${MODULE_NAME}" \
-hiveconf hive.querylog.location="${LOG_DIR_MODULE}/${module_log_file}" \
-i "${HIVE_INITIALISATION_FILE}" \
-e  "use ${hive_database_name_with_prefix}; ALTER TABLE ${HIVE_TABLE_PREFIX}${hive_table} DROP PARTITION (${first_partition},${second_partition});" $@ 1>> "${mobule_out_file}" 2>> "${module_err_file}"

exit_code=$?

fail_on_error=$BOOLEAN_TRUE

success_message="Successfully drop parition ${first_partition}/${second_partition} to hive table ${hive_table}"

failure_message="Failed to drop parition ${first_partition}/${second_partition} to hive table ${hive_table}"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

hive -d first.partition="${first_partition}" \
-d second.partition="${second_partition}" \
-hiveconf ${UNIQUE_PROPERTY_PREFIX}.first.partition="${first_partition}" \
-hiveconf ${UNIQUE_PROPERTY_PREFIX}.second.partition="${second_partition}" \
-hiveconf ${UNIQUE_PROPERTY_PREFIX}.subject.area.name="${SUBJECT_AREA_NAME}" \
-hiveconf ${UNIQUE_PROPERTY_PREFIX}.project.name="${PROJECT_NAME}" \
-hiveconf ${UNIQUE_PROPERTY_PREFIX}.module.name="${MODULE_NAME}" \
-hiveconf hive.querylog.location="${LOG_DIR_MODULE}/${module_log_file}" \
-i "${HIVE_INITIALISATION_FILE}" \
-e  "use ${hive_database_name_with_prefix}; ALTER TABLE ${HIVE_TABLE_PREFIX}${hive_table} ADD PARTITION (${first_partition},${second_partition});" $@ 1>> "${mobule_out_file}" 2>> "${module_err_file}"

exit_code=$?

fail_on_error=$BOOLEAN_TRUE

success_message="Successfully added parition ${first_partition}/${second_partition} to hive table ${hive_table}"

failure_message="Failed to add parition ${first_partition}/${second_partition} to hive table ${hive_table}"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"
}



##
# Execute hive script
# Arguments:
#   pass any arguments that you want to pass to hive
#   command
#
#
function fn_move_raw_file_from_unix_to_hdfs_raw_table(){

fn_get_current_batch_id

fn_get_module_log_dir_and_file

unix_dir_path="$1"

hive_database_name="$2"

hive_table_name="$3"

partition_batch_id="$4"

assert_variable_is_set "hive_database" "${hive_database_name}"

assert_variable_is_set "hive_table" "${hive_table_name}"

assert_variable_is_set "partition_batch_id" "${partition_batch_id}"

hive_database_name_with_prefix="${HIVE_DATABASE_PREFIX}${hive_database_name}"

module_type="hive"

module_log_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}"

mobule_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"

module_err_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.err"

HIVE_INITIALISATION_FILE="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.rc"

fn_generate_hive_initialization_file

fn_log_info "Log file : ${module_log_file}"

fn_log_info "Out file : ${mobule_out_file}"

fn_log_info "Err file : ${module_err_file}"

hive -d  batch.id="${BATCH_ID}" \
-hiveconf database="${HIVE_DATABASE_PREFIX}${hive_database_name}" \
-hiveconf ${UNIQUE_PROPERTY_PREFIX}.batch.id="${BATCH_ID}" \
-hiveconf ${UNIQUE_PROPERTY_PREFIX}.subject.area.name="${SUBJECT_AREA_NAME}" \
-hiveconf ${UNIQUE_PROPERTY_PREFIX}.project.name="${PROJECT_NAME}" \
-hiveconf ${UNIQUE_PROPERTY_PREFIX}.module.name="${MODULE_NAME}" \
-hiveconf hive.querylog.location="${LOG_DIR_MODULE}/${module_log_file}" \
-i "${HIVE_INITIALISATION_FILE}" \
-e  "LOAD DATA LOCAL INPATH '${unix_dir_path}' INTO TABLE ${hive_database_name_with_prefix}.${HIVE_TABLE_PREFIX}${hive_table_name}
PARTITION (batch_id='${partition_batch_id}');" 1>> "${mobule_out_file}" 2>> "${module_err_file}"


exit_code=$?

fail_on_error=$BOOLEAN_TRUE

success_message="Successfully moved file from ${unix_dir_path} to hive table ${HIVE_TABLE_PREFIX}${hive_table_name} in partition ${partition_batch_id}"

failure_message="Failed to move file from ${unix_dir_path} to hive table ${HIVE_TABLE_PREFIX}${hive_table_name} in partition ${partition_batch_id}"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"
}


##
# Execute pig script
#
# Arguments:
#   pass any arguments that you want to pass to pig command
#
#
function fn_execute_mapreduce(){

jar_path="$1"
assert_variable_is_set "jar_path" "${jar_path}"

fn_get_current_batch_id

fn_get_module_log_dir_and_file

module_type="mapreduce"

module_log_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}"

mobule_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"

module_err_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.err"

fn_log_info "Log file : ${module_log_file}"

fn_log_info "Out file : ${mobule_out_file}"

fn_log_info "Err file : ${module_err_file}"
# hadoop jar ${jar_path} \
hadoop  \
jar "$@" \
-D ${UNIQUE_PROPERTY_PREFIX}.batch.id="${BATCH_ID}" \
-D ${UNIQUE_PROPERTY_PREFIX}.subject.area.name="${SUBJECT_AREA_NAME}" \
-D ${UNIQUE_PROPERTY_PREFIX}.project.name="${PROJECT_NAME}" \
-D ${UNIQUE_PROPERTY_PREFIX}.module.name="${MODULE_NAME}" \
-D job.name="${MODULE_NAME}" \
1>> "${mobule_out_file}" 2>> "${module_err_file}"

exit_code=$?

fail_on_error=$BOOLEAN_TRUE

success_message="Successfully executed mapreduce job ${jar_path}"

failure_message="Mapreduce job ${jar_path} failed"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}



##
# Execute pig script
#
# Arguments:
#   pass any arguments that could be passed to pig command
#
#
function fn_execute_pig(){

fn_get_current_batch_id

fn_get_module_log_dir_and_file

module_type="pig"

module_log_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}"

mobule_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"

module_err_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.err"

fn_log_info "Log file : ${module_log_file}"

fn_log_info "Out file : ${mobule_out_file}"

fn_log_info "Err file : ${module_err_file}"


#USER_NAMESPACE_PROPERTIES_FILE parameter is defined in all module scripts
user_namespace_param_file_option="${USER_NAMESPACE_PROPERTIES_FILE}"

if [ ! -f "$USER_NAMESPACE_PROPERTIES_FILE" ];
then

user_namespace_param_file_option="${HOME}/etc/namespace.properties"

fi

if [ "${ENABLE_PIG_REMOTE_DEBUGGING}" == "true" ]
then

INTERNAL_PIG_OPTS="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=${PIG_REMOTE_DEBUGGING_PORT},suspend=y"

fi

if [ "${ENABLE_PIG_INSTRUMENTATION}" == "true" ]
then

PIG_INSTRUMENTATION_AGENT_OPTS="-javaagent:${REPOSITORY_DIR}/${PIG_INSTRUMENTATION_AGENT_JAR}"

PIG_STATS_JAR_PATH="${REPOSITORY_DIR}/${PIG_STATS_JAR}"

fi

export PIG_OPTS="${PIG_INSTRUMENTATION_AGENT_OPTS} ${INTERNAL_PIG_OPTS} ${PIG_OPTS}"

export PIG_CLASSPATH="${PIG_STATS_JAR_PATH}:${PIG_CLASSPATH}"

#if [ ! -f "${}" ]
#then
if [ ${CHILD_MODULE} == "" ]
then
use_child_module_pig_properties=" "
else
#source "${MODULE_HOME}/etc/${CHILD_MODULE}module.env.properties"
use_child_module_pig_properties="-m ${MODULE_HOME}/etc/${CHILD_MODULE}.module.pig.properties "
fi

pig -Dudf.import.list="${PIG_UDF_IMPORT_LIST}"  \
-Dunique.property.prefix="${UNIQUE_PROPERTY_PREFIX}" \
-D${UNIQUE_PROPERTY_PREFIX}.batch.id="${BATCH_ID}" \
-D${UNIQUE_PROPERTY_PREFIX}.subject.area.name="${SUBJECT_AREA_NAME}" \
-D${UNIQUE_PROPERTY_PREFIX}.project.name="${PROJECT_NAME}" \
-D${UNIQUE_PROPERTY_PREFIX}.module.name="${MODULE_NAME}" \
-Djob.name="${MODULE_NAME}" \
-p BATCH_ID="${BATCH_ID}"  \
-useHCatalog \
-logfile "${module_log_file}" \
-m "${user_namespace_param_file_option}" \
-m "${HOME}/etc/default.pig.properties" \
-m "${SUBJECT_AREA_HOME}/etc/subject-area.pig.properties" \
-m "${PROJECT_HOME}/etc/project.pig.properties" \
-m "${MODULE_HOME}/etc/module.pig.properties" \
${use_child_module_pig_properties} "$@" 1>> "${module_err_file}" 2>> "${mobule_out_file}"

exit_code=$?

fail_on_error=$BOOLEAN_TRUE

success_message="Successfully executed ${MODULE_NAME} pig script "

failure_message="Pig script ${MODULE_NAME} failed"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}


function fn_execute_dim_pig(){

fn_get_current_batch_id

fn_get_module_log_dir_and_file

module_type="pig"

module_log_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}"

mobule_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"

module_err_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.err"

fn_log_info "Log file : ${module_log_file}"

fn_log_info "Out file : ${mobule_out_file}"

fn_log_info "Err file : ${module_err_file}"


#USER_NAMESPACE_PROPERTIES_FILE parameter is defined in all module scripts
user_namespace_param_file_option="${USER_NAMESPACE_PROPERTIES_FILE}"

if [ ! -f "$USER_NAMESPACE_PROPERTIES_FILE" ];
then

user_namespace_param_file_option="${HOME}/etc/namespace.properties"

fi

if [ "${ENABLE_PIG_REMOTE_DEBUGGING}" == "true" ]
then

INTERNAL_PIG_OPTS="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=${PIG_REMOTE_DEBUGGING_PORT},suspend=y"

fi

if [ "${ENABLE_PIG_INSTRUMENTATION}" == "true" ]
then

PIG_INSTRUMENTATION_AGENT_OPTS="-javaagent:${REPOSITORY_DIR}/${PIG_INSTRUMENTATION_AGENT_JAR}"

PIG_STATS_JAR_PATH="${REPOSITORY_DIR}/${PIG_STATS_JAR}"

fi

export PIG_OPTS="${PIG_INSTRUMENTATION_AGENT_OPTS} ${INTERNAL_PIG_OPTS} ${PIG_OPTS}"

export PIG_CLASSPATH="${PIG_STATS_JAR_PATH}:${PIG_CLASSPATH}"

#if [ ! -f "${}" ]
#then
if [ ${CHILD_MODULE} == "" ]
then
use_child_module_pig_properties=" "
else
#source "${MODULE_HOME}/etc/${CHILD_MODULE}module.env.properties"
use_child_module_pig_properties="-m ${CHILD_MODULE_HOME}/etc/${CHILD_MODULE}.module.pig.properties "
fi

pig -Dudf.import.list="${PIG_UDF_IMPORT_LIST}"  \
-Dunique.property.prefix="${UNIQUE_PROPERTY_PREFIX}" \
-D${UNIQUE_PROPERTY_PREFIX}.batch.id="${BATCH_ID}" \
-D${UNIQUE_PROPERTY_PREFIX}.subject.area.name="${SUBJECT_AREA_NAME}" \
-D${UNIQUE_PROPERTY_PREFIX}.project.name="${PROJECT_NAME}" \
-D${UNIQUE_PROPERTY_PREFIX}.module.name="${MODULE_NAME}" \
-Djob.name="${MODULE_NAME}" \
-p BATCH_ID="${BATCH_ID}"  \
-useHCatalog \
-logfile "${module_log_file}" \
-m "${user_namespace_param_file_option}" \
-m "${HOME}/etc/default.pig.properties" \
-m "${SUBJECT_AREA_HOME}/etc/subject-area.pig.properties" \
-m "${PROJECT_HOME}/etc/project.pig.properties" \
-m "${MODULE_HOME}/etc/module.pig.properties" \
-m "${CHILD_MODULE_HOME}/etc/${CHILD_MODULE}.module.pig.properties" \
${use_child_module_pig_properties} "$@" 1>> "${module_err_file}" 2>> "${mobule_out_file}"

exit_code=$?

fail_on_error=$BOOLEAN_TRUE

success_message="Successfully executed ${MODULE_NAME} pig script "

failure_message="Pig script ${MODULE_NAME} failed"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}


##
# Execute hive script
# Arguments:
#   pass any arguments that you want to pass to hive
#   command
#
#
function fn_execute_hive(){

fn_get_current_batch_id

fn_get_module_log_dir_and_file


hive_script_file="$1"

assert_variable_is_set "hive_script_file" "${hive_script_file}"

module_type="hive"

module_log_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}"
mobule_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"
module_err_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.err"

HIVE_INITIALISATION_FILE="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.rc"
fn_generate_hive_initialization_file

fn_log_info "Log file : ${module_log_file}"
fn_log_info "Out file : ${mobule_out_file}"
fn_log_info "Err file : ${module_err_file}"

hive -d batch.id="${BATCH_ID}" \
-hiveconf ${UNIQUE_PROPERTY_PREFIX}.batch.id="${BATCH_ID}" \
-hiveconf ${UNIQUE_PROPERTY_PREFIX}.subject.area.name="${SUBJECT_AREA_NAME}" \
-hiveconf ${UNIQUE_PROPERTY_PREFIX}.project.name="${PROJECT_NAME}" \
-hiveconf ${UNIQUE_PROPERTY_PREFIX}.module.name="${MODULE_NAME}" \
-hiveconf hive.querylog.location="${LOG_DIR_MODULE}/${module_log_file}" \
-i "${HIVE_INITIALISATION_FILE}" \
-f "${hive_script_file}" "${@:2}" 1>> "${module_err_file}" 2>> "${mobule_out_file}"

exit_code=$?

fail_on_error=$BOOLEAN_TRUE

success_message="Successfully executed ${hive_script_file} hive script"

failure_message="Failed to execute ${hive_script_file} hive script"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

function fn_execute_hive_for_error_records(){

fn_get_current_batch_id

fn_get_module_log_dir_and_file


hive_script_file="$1"

assert_variable_is_set "hive_script_file" "${hive_script_file}"

module_type="hive"

module_log_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}"
mobule_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"
module_err_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.err"

HIVE_INITIALISATION_FILE="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.rc"
fn_generate_hive_initialization_file

fn_log_info "Log file : ${module_log_file}"
fn_log_info "Out file : ${mobule_out_file}"
fn_log_info "Err file : ${module_err_file}"

hive -d batch.id="${BATCH_ID}" \
-hiveconf batch_id="${BATCH_ID}" \
-hiveconf ${UNIQUE_PROPERTY_PREFIX}.subject.area.name="${SUBJECT_AREA_NAME}" \
-hiveconf ${UNIQUE_PROPERTY_PREFIX}.project.name="${PROJECT_NAME}" \
-hiveconf module_name="${MODULE_NAME}" \
-hiveconf unique_property_prefix="${UNIQUE_PROPERTY_PREFIX}" \
-hiveconf hive.querylog.location="${LOG_DIR_MODULE}/${module_log_file}" \
-i "${HIVE_INITIALISATION_FILE}" \
-f "${hive_script_file}" "${@:2}" 1> "${module_err_file}" 2>> "${mobule_out_file}"

exit_code=$?

fail_on_error=$BOOLEAN_TRUE

success_message="Successfully executed ${hive_script_file} hive script"

failure_message="Failed to execute ${hive_script_file} hive script"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

error_table_count_dq=`cat $module_err_file|wc -l`

if [ $error_table_count_dq -ne 0 ]
then
#   echo "This is to notify you that attached records rejected while loading data_quality_check for ${MODULE_NAME}, Path : $module_err_file and Error record count : ${error_table_count_dq}"| mailx -r "Express CDW Alerts <expresscdwalerts@express.com>" -s "Records moved to error table while loading ${MODULE_NAME}" -a $module_err_file aman.jain@datametica.com sumit.roy@datametica.com aditi.chauhan@datametica.com akshay.rochwani@datametica.com amruta.chalakh@datametica.com anish.nair@datametica.com bhautik.patel@datametica.com dewakar.prasad@datametica.com gauravkumar.maheshwari@datametica.com hemanth.reddy@datametica.com mahendranadh.dasari@datametica.com mayur.badgujar@datametica.com neha.mahajan@datametica.com poonam.mishra@datametica.com shashi.prasad@datametica.com surbhi.bhandari@datametica.com

echo "This is to notify you that attached records rejected while loading data_quality_check for ${MODULE_NAME}, Path : $module_err_file and Error record count : ${error_table_count_dq}"| mailx -r "Express CDW Alerts <expresscdwalerts@express.com>" -s "Records moved to error table while loading ${MODULE_NAME}" -a $module_err_file ${EMAIL_GROUP}

else
echo "Successfully executed dataquality without error "
fi


}


function fn_execute_impala(){

fn_get_current_batch_id

fn_get_module_log_dir_and_file


hive_script_file="$1"

assert_variable_is_set "hive_script_file" "${hive_script_file}"

module_type="hive"

module_log_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}"
mobule_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"
module_err_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.err"

HIVE_INITIALISATION_FILE="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.rc"
fn_generate_hive_initialization_file

fn_log_info "Log file : ${module_log_file}"
fn_log_info "Out file : ${mobule_out_file}"
fn_log_info "Err file : ${module_err_file}"


impala-shell -i $IMPALA_NODE -q "${@}" 1>> "${mobule_out_file}" 2>> "${module_err_file}"

exit_code=$?

fail_on_error=$BOOLEAN_TRUE

success_message="Successfully executed ${hive_script_file} hive script"

failure_message="Failed to execute ${hive_script_file} hive script"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}


##
# Comapct all files of one directory and
# write into another directory
#
# Arguments:
#   pass any arguments that you want to pass to pig command
#
#

function fn_compact(){

base_dir=$1
in_dir=$2
out_dir=$3

assert_variable_is_set "base_dir" "${base_dir}"
assert_variable_is_set "in_dir" "${in_dir}"
assert_variable_is_set "out_dir" "${out_dir}"

fn_get_current_batch_id

non_compact_directory=${base_dir}/${in_dir}
compact_directory=${base_dir}/${out_dir}
compact_batchid_directory=${base_dir}/$BATCH_ID

declare -a fetch_dir
OLD_IFS=${IFS}
IFS=$'\n'

success_message="the directory ${non_compact_directory} exists"
failure_message="the directory ${non_compact_directory} doesn't exist"
fail_on_error=$BOOLEAN_TRUE

hdfs dfs -test -e "${non_compact_directory}"
exit_code=$?

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

fetch_dir=($(hadoop fs -ls ${non_compact_directory}))
assert_variable_is_set "fetch_dir" "${fetch_dir}"
unset fetch_dir[${#fetch_dir[@]}-1]
counter_fetch_dir=${#fetch_dir[@]}

exit_code=0
if [ $counter_fetch_dir -le 1 ]
then
exit_code=1
fi

fn_handle_exit_code \
"${exit_code}" \
"$counter_fetch_dir previous directories exist" \
"only one directory exists. exiting as flume may be writing to it" "${fail_on_error}"

dir_index=1
while [ "$dir_index" -lt $counter_fetch_dir ]
do
fn_log_info "running compaction for ${fetch_dir[dir_index]}"

hour_bucket=`echo ${fetch_dir[dir_index]} | sed 's,^.*/,,'`
files_in_hour_bucket=`(hadoop fs -ls $DATA_LAYER_DIR_INCOMING_RAW/$GROUP_NAME_DIR/events/$hour_bucket) | wc -l`

pig_input_path="${non_compact_directory}/$hour_bucket"
pig_output_path="${compact_directory}/$hour_bucket"

if [ $files_in_hour_bucket -ge 1 ];
then
fn_get_current_batch_id

fn_log_info "deleting compaction pig script output directory: ${pig_output_path}"
fn_hadoop_delete_directory_if_exists \
"${pig_output_path}"

fn_execute_pig \
-param pig_input_path="${pig_input_path}" \
-param pig_output_path="${pig_output_path}"  \
"${COMMON_MODULES_HOME}/pig-compaction/pig/module-prepare.pig"

if [  $? == 0 ];
then
fn_log_info "compaction performed for: ${pig_input_path}"
fn_hadoop_delete_directory_if_exists \
"${pig_input_path}"
fi

else
fn_log_info "${pig_input_path} seems to be blank"
fn_hadoop_delete_directory_if_exists "${pig_input_path}"
fi

dir_index=`expr $dir_index + 1`

done

IFS=${OLD_IFS}

fn_hadoop_delete_directory_if_exists \
"${compact_batchid_directory}" \

fn_hadoop_create_directory_if_not_exists \
"${compact_batchid_directory}" \

source_dir_count=`(hadoop fs -ls $compact_directory) | wc -l`

if [ $source_dir_count -ge 1 ];
then
fn_hadoop_move_file_or_directory \
"${compact_directory}/*" \
"${compact_batchid_directory}"
else
fn_log_info "no folders found in $compact_directory"
exit 1
fi
}

function fn_execute_sqoop_export_to_teradata(){
teradata_db=$2
source_table_path=$3
target_table_name=$4
staging_option=$5

fn_get_current_batch_id

fn_get_module_log_dir_and_file

module_type="sqoop"

sqoop_module_log_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}"

sqoop_mobule_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"

sqoop_module_err_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.err"

fn_log_info "Log file : ${sqoop_module_log_file}"

fn_log_info "Out file : ${sqoop_mobule_out_file}"

fn_log_info "Err file : ${sqoop_module_err_file}"

#USER_NAMESPACE_PROPERTIES_FILE parameter is defined in all module scripts
user_namespace_param_file_option="${USER_NAMESPACE_PROPERTIES_FILE}"

if [ ! -f "$USER_NAMESPACE_PROPERTIES_FILE" ];
then
user_namespace_param_file_option="${HOME}/etc/namespace.properties"
fi

if [ "${staging_option}" = "staging" ]
then
echo "exporting $target_table_name table to Teradata with staging-table."
sqoop export \
--connect "${teradata_connect_string}=${teradata_db}" \
--connection-manager "${teradata_driver}" \
--username "${teradata_username}" \
--password "${teradata_password}" \
--table "${target_table_name}" \
--staging-table "${target_table_name}_staging" \
--export-dir "${source_table_path}" \
--fields-terminated-by "${delimiter}" 2> "${sqoop_module_err_file}" 1> "${sqoop_mobule_out_file}"
else
echo "exporting $target_table_name table to Teradata."
sqoop export \
--connect "${teradata_connect_string}=${teradata_db}" \
--connection-manager "${teradata_driver}" \
--username "${teradata_username}" \
--password "${teradata_password}" \
--table "${target_table_name}" \
--export-dir "${source_table_path}" \
--fields-terminated-by "${delimiter}" 2> "${sqoop_module_err_file}" 1> "${sqoop_mobule_out_file}"
fi

exit_code_export=$?

fail_on_error=$BOOLEAN_TRUE

fn_email_status "${exit_code_export}" "${target_table_name}" "${sqoop_mobule_out_file}" "${sqoop_module_err_file}"

success_message="Successfully export table ${target_table_name} from hive to teradata database"

failure_message="Failed to export table ${target_table_name} to teradata database from hive."

fn_handle_exit_code "${exit_code_export}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

function fn_execute_sqoop_export_to_db2(){
db2_db=$2
source_table_path=$3
schema_target_table_name=$4

fn_get_current_batch_id

fn_get_module_log_dir_and_file

module_type="sqoop"

sqoop_module_log_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}"

sqoop_mobule_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"

sqoop_module_err_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.err"

fn_log_info "Log file : ${module_log_file}"

fn_log_info "Out file : ${mobule_out_file}"

fn_log_info "Err file : ${module_err_file}"

#USER_NAMESPACE_PROPERTIES_FILE parameter is defined in all module scripts
user_namespace_param_file_option="${USER_NAMESPACE_PROPERTIES_FILE}"

if [ ! -f "$USER_NAMESPACE_PROPERTIES_FILE" ];
then
user_namespace_param_file_option="${HOME}/etc/namespace.properties"
fi

echo "exporting $target_table_name table to DB2."
sqoop export \
--driver "${db2_driver}" \
--connect "${db2_connect_string}${db2_db}" \
--username "${db2_username}" \
--password "${db2_password}" \
--table "${schema_target_table_name}" \
--export-dir "${source_table_path}" \
--fields-terminated-by "${delimiter}" 2> "${sqoop_module_err_file}" 1> "${sqoop_mobule_out_file}"

exit_code_export=$?

fail_on_error=$BOOLEAN_TRUE

fn_email_status "${exit_code_export}" "${schema_target_table_name}" "${sqoop_mobule_out_file}" "${sqoop_module_err_file}"

success_message="Successfully export table ${target_table_name} from hive to DB2 database"

failure_message="Failed to export table ${target_table_name} to DB2 database from hive."

fn_handle_exit_code "${exit_code_export}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

function fn_execute_sqoop_export(){
target="$1"

case "${target}" in

teradata)

echo "################################################# using teradata.#######################################################"

fn_execute_sqoop_export_to_teradata "$@"

;;

oracle)

fn_execute_sqoop_export_to_oracle "$@"

;;

db2)

echo "################################################# using DB2. ############################################################"

fn_execute_sqoop_export_to_db2 "$@"

;;
*)

echo $"Usage: $0 {teradata|oracle|db2}"

exit 1

esac

}


function fn_execute_sqoop_import_from_teradata(){
teradata_db=$2
hive_table_path=$3
source_table_name=$4
pk_for_import=$5

fn_get_current_batch_id

fn_get_module_log_dir_and_file

module_type="sqoop"

sqoop_module_log_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}"

sqoop_mobule_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"

sqoop_module_err_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.err"

fn_log_info "Log file : ${sqoop_module_log_file}"

fn_log_info "Out file : ${sqoop_mobule_out_file}"

fn_log_info "Err file : ${sqoop_module_err_file}"

#USER_NAMESPACE_PROPERTIES_FILE parameter is defined in all module scripts
user_namespace_param_file_option="${USER_NAMESPACE_PROPERTIES_FILE}"

if [ ! -f "$USER_NAMESPACE_PROPERTIES_FILE" ];
then
user_namespace_param_file_option="${HOME}/etc/namespace.properties"
fi
echo "Importing $source_table_name table from Teradata with."
sqoop import \
--connect "${teradata_connect_string}=${teradata_db}" \
--connection-manager "${teradata_driver}" \
--username "${teradata_username}" \
--password "${teradata_password}" \
--split-by "${pk_for_import}" \
--table "${source_table_name}" \
--target-dir "${hive_table_path}" \
--hive-overwrite \
--null-string '' \
--null-non-string '' \
--fields-terminated-by "${delimiter}" 2> "${sqoop_module_err_file}" 1> "${sqoop_mobule_out_file}"

exit_code_import=$?

fail_on_error=$BOOLEAN_TRUE

fn_email_status "${exit_code_import}" "${source_table_name}" "${sqoop_mobule_out_file}" "${sqoop_module_err_file}"

success_message="Successfully imported table ${target_table_name} from teradata to hive database"

failure_message="Failed to import table ${target_table_name} from teradata to hive database."

fn_handle_exit_code "${exit_code_import}" "${success_message}" "${failure_message}" "${fail_on_error}"

echo "$exit_code"

}

function fn_execute_sqoop_import_from_db2(){
db2_db=$2
hive_table_path=$3
schema_source_table_name=$4
pk_for_import=$5

fn_get_current_batch_id

fn_get_module_log_dir_and_file

module_type="sqoop"

sqoop_module_log_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}"

sqoop_mobule_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"

sqoop_module_err_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.err"

fn_log_info "Log file : ${sqoop_module_log_file}"

fn_log_info "Out file : ${sqoop_mobule_out_file}"

fn_log_info "Err file : ${sqoop_module_err_file}"

#USER_NAMESPACE_PROPERTIES_FILE parameter is defined in all module scripts
user_namespace_param_file_option="${USER_NAMESPACE_PROPERTIES_FILE}"

if [ ! -f "$USER_NAMESPACE_PROPERTIES_FILE" ];
then
user_namespace_param_file_option="${HOME}/etc/namespace.properties"
fi

echo "importing $schema_source_table_name table from DB2."
sqoop import \
--driver "${db2_driver}" \
--connect "${db2_connect_string}${db2_db}" \
--username "${db2_username}" \
--password "${db2_password}" \
--split-by "${pk_for_import}" \
--table "${schema_source_table_name}" \
--target-dir "${hive_table_path}" \
--hive-overwrite \
--null-string '' \
--null-non-string '' \
--fields-terminated-by "${delimiter}" 2> "${sqoop_module_err_file}" 1> "${sqoop_mobule_out_file}"

exit_code_import=$?

fail_on_error=$BOOLEAN_TRUE

fn_email_status "${exit_code_import}" "${source_table_name}" "${sqoop_mobule_out_file}" "${sqoop_module_err_file}"

success_message="Successfully imported table ${schema_source_table_name} from DB2 to hive database"

failure_message="Failed to import table ${schema_source_table_name} from DB2 database to hive."

fn_handle_exit_code "${exit_code_import}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

function fn_execute_sqoop_import_from_sql_server(){
fn_get_current_batch_id
fn_get_module_log_dir_and_file

module_name="sqoop"

sqoop_module_log_file="${LOG_DIR_MODULE}/${module_name}.${LOG_FILE_NAME_MODULE}"
sqoop_mobule_out_file="${LOG_DIR_MODULE}/${module_name}.${LOG_FILE_NAME_MODULE}.out"
sqoop_module_err_file="${LOG_DIR_MODULE}/${module_name}.${LOG_FILE_NAME_MODULE}.err"

fn_log_info "Log file : ${sqoop_module_log_file}"
fn_log_info "Out file : ${sqoop_mobule_out_file}"
fn_log_info "Err file : ${sqoop_module_err_file}"

#USER_NAMESPACE_PROPERTIES_FILE parameter is defined in all module scripts
user_namespace_param_file_option="${USER_NAMESPACE_PROPERTIES_FILE}"

if [ ! -f "$USER_NAMESPACE_PROPERTIES_FILE" ];
then
user_namespace_param_file_option="${HOME}/etc/namespace.properties"
fi

source_database=$1
source_table=$2
fields_terminated_by=$3
split_by_column=$4
num_of_mappers=$5
target_dir=$6
where_conditions_for_incremental=$7
extra_arguments="${@:8}"

source "${HOME}/etc/default.sqoop.properties"
source "${SUBJECT_AREA_HOME}/etc/subject-area.sqoop.properties"
source "${PROJECT_HOME}/etc/project.sqoop.properties"

sqoop import \
--driver "${sql_server_driver}" \
--connect "${sql_server_connect_string};database=${source_database}" \
--username "${sql_server_username}" \
--password "${sql_server_password}" \
--fields-terminated-by "${fields_terminated_by}" \
--split-by "${split_by_column}" \
--query "SELECT * FROM ${source_table} WHERE ${where_conditions_for_incremental} \$CONDITIONS" \
-m $((num_of_mappers)) \
--target-dir "${target_dir}" ${extra_arguments[@]} 2> "${sqoop_module_err_file}" 1> "${sqoop_mobule_out_file}"

exit_code_import=$?

fail_on_error=$BOOLEAN_TRUE

success_message="Successfully imported table ${schema_source_table_name} from sql_server to hive database"
failure_message="Failed to import table ${schema_source_table_name} from sql_server database to hive. "

fn_handle_exit_code "${exit_code_import}" "${success_message}" "${failure_message}" "${fail_on_error}"
}

function fn_execute_sqoop_import(){

source="$1"

case "${source}" in

teradata)

echo "################################################# using teradata.#######################################################"

fn_execute_sqoop_import_from_teradata "$@"

;;

oracle)

fn_execute_sqoop_import_from_oracle "$@"

;;

db2)

echo "################################################# using DB2. ############################################################"

fn_execute_sqoop_import_from_db2 "$@"

;;

*)
echo $"Usage: $0 {teradata|oracle|db2}"
exit 1
esac
}

function fn_get_hivetable_properties(){
db_name=$1
table_name=$2
table_property=$3
if [ -z "${table_property}" ]
then
db_name=$1
table_name=$2
table_properties=$(
hive \
-S \
-hiveconf db=$db_name \
-hiveconf table=$table_name \
-e 'show TBLPROPERTIES ${hiveconf:db}.${hiveconf:table}')
else
table_properties=$(
hive \
-S \
-hiveconf tp=$table_property \
-hiveconf db=$db_name \
-hiveconf table=$table_name \
-e 'show TBLPROPERTIES ${hiveconf:db}.${hiveconf:table}("${hiveconf:tp}")')
fi

exit_code=$?

fail_on_error=$BOOLEAN_TRUE

success_message="Properties fetched successfully."

failure_message="Failed to get properties."

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

##
# Perform setup tasks for this module
#
#
function fn_module_setup(){

fn_log_warn "Operation not implemented"

}

##
# Mark the input for this job
#
function fn_module_mark(){

fn_log_warn "Operation not implemented"

}


##
# Execute preparation script for this module
#
#
function fn_module_prepare(){

fn_log_warn "Operation not implemented"

}


##
# Execute business logic
#
#
function fn_module_transform(){

fn_log_warn "Operation not implemented"

}

##
# Execute enrichment logic
#
#
function fn_module_enrich(){

fn_log_warn "Operation not implemented"

}

##
# Validate the data processed by module
#
#
function fn_module_validate(){

fn_log_warn "Operation not implemented"

}



##
# Validate the data processed by module
#
#
function fn_module_export(){

fn_log_warn "Operation not implemented"

}



##
# Perform post module scripts execution tasks
#
#
function fn_module_cleanup(){

fn_log_warn "Operation not implemented"

}


##
# Entry point for this module.
# It executes operation passed on the command line
# as the first argument while executing the script.
#
#
function fn_main(){

command="$1"

case "${command}" in

setup)

fn_module_setup "${@:2}"

;;

setupWork)

fn_module_setupWork "${@:2}"

;;

setupGold)

fn_module_setupGold "${@:2}"

;;


mark)

fn_module_mark "${@:2}"

;;

prepare)

fn_module_prepare "${@:2}"

;;

transform)

fn_module_transform "${@:2}"

;;

enrich)

fn_module_enrich "${@:2}"

;;

validate)

fn_module_validate "${@:2}"

;;

export)

fn_module_export "${@:2}"

;;

cleanup)

fn_module_cleanup "${@:2}"

;;

backup)

fn_module_backup "${@:2}"

;;

load)

fn_module_load "${@:2}"

;;

file_trans)

fn_module_hdfs_file_trans "${@:2}"

;;
preprocess)

fn_main_project "${@:2}"

;;

get)

fn_getfile "${@:2}"

;;

put)

fn_putfile "${@:2}"

;;

getLw)

fn_get_files_from_sftp "${@:2}"

;;

putLw)

fn_put_files_to_sftp "${@:2}"

;;

encrypt)

fn_encrypt "${@:2}"

;;
decrypt)

fn_decrypt "${@:2}"

;;

*)

echo $"Usage: $0 {setup|setupWork|setupGold|mark|prepare|transform|validate|export|cleanup|backup|load|hdfs_file_trans|get|put|encrypt|decrypt|preprocess}"


exit 1

esac

}

function fn_xlsx2csv(){  # Need to handle failure conditions

file_name=$1

to_directory=$2

fail_on_error=$3

assert_variable_is_set "file_name" "${file_name}"
assert_variable_is_set "to_directory" "${to_directory}"
xlsx2csv ${file_name} "${to_directory}"
exit_code=$?
success_message="Successfully converted xls file from ${file_name} to ${to_directory}"
failure_message="Failed to convert xls file from ${file_name} to ${to_directory}"
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"
}


function get_spark_property_file(){
if [ -f "${MODULE_HOME}/${CHILD_MODULE}/etc/spark.properties" ];
then
SPARK_PROP_FILE="${MODULE_HOME}/${CHILD_MODULE}/etc/spark.properties"
else
SPARK_PROP_FILE="${MODULE_HOME}/etc/spark.properties"
fi
fn_log_info "Spark property file: $SPARK_PROP_FILE"
}

# It executes Dim_member load operation after Customer matching
function fn_execute_DimMemberSpark(){

CHILD_MODULE_HOME="$1"
CHILD_MODULE="$2"
DIMENTION_FILE="$3"

fn_get_current_batch_id

fn_get_module_log_dir_and_file

module_type="spark"


mobule_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"


fn_log_info "Out file : ${mobule_out_file}"


if [ -f "${CHILD_MODULE_HOME}/etc/${CHILD_MODULE}.module.env.properties" ];
then
. ${CHILD_MODULE_HOME}/etc/${CHILD_MODULE}.module.env.properties
fi

if [ -f "${CHILD_MODULE_HOME}/etc/${CHILD_MODULE}.module.pig.properties" ];
then
. ${CHILD_MODULE_HOME}/etc/${CHILD_MODULE}.module.pig.properties
fi

if [ -f "${CHILD_MODULE_HOME}/etc/${DIMENTION_FILE}.module.pig.properties" ];
then
. ${CHILD_MODULE_HOME}/etc/${DIMENTION_FILE}.module.pig.properties
fi

get_spark_property_file

spark-submit --master ${SPARK_MASTER}  --deploy-mode ${SPARK_DEPLOY_MODE} --jars ${SPARK_EXTRA_JARS} --properties-file ${SPARK_PROP_FILE} --class ${PROCESSFILE_CLASS} --driver-java-options "${SPARK_ECAT_PROP} -Dlog4j.configuration=log4j.properties -Dcustomer_matching.lookup_db=${LOOKUP_DB} -Dcustomer_matching.work_db=${WORK_DB}" ${PROCESSFILE_JAR_PATH} ${DIMENTION_FILE} ${PROCESSFILE_DB} ${PROCESSFILE_TABLE} \ "$@" >> ${mobule_out_file} 2>&1


fail_on_error=$BOOLEAN_TRUE

success_message="Successfully executed ${MODULE_NAME} spark script "

failure_message="Spark script ${MODULE_NAME} failed"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

# It executes Customer matching
function fn_execute_CustomerMatchingSpark(){

LookupDb="$1"
work_db="$2"
CoalescePartitions="$3"
MODULE_HOME_CM="$4"
child_module="$5"

current_dt=`echo "$(date +'%Y-%m-%d')"`
module_err_file=/var/tmp/
#EMAIL_GROUP=expithadoopsupportteam@express.com,dmexpressoffshore@datametica.com
TLOG_E_LIST=/var/tmp/tlogExceptionList.txt
CUSTOMER_E_LIST=/var/tmp/customerExceptionList.txt
BP_MEMBER_E_LIST=/var/tmp/bpmemberExceptionList.txt


fn_get_current_batch_id

fn_get_module_log_dir_and_file

module_type="spark"

mobule_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"

fn_log_info "Out file : ${mobule_out_file}"


if [  -f "${MODULE_HOME_CM}/etc/module.env.properties" ];
then
. ${MODULE_HOME_CM}/etc/module.env.properties
fi

if [  -f "${MODULE_HOME_CM}/etc/${child_module}.module.env.properties" ];
then
. ${MODULE_HOME_CM}/etc/${child_module}.module.env.properties
fi

get_spark_property_file

spark-submit  --master ${SparkMaster}  --deploy-mode ${SparkDeployMode} --files ${SPARK_EXTRA_FILES} --jars ${SPARK_EXTRA_JARS} --properties-file ${SPARK_PROP_FILE} --class ${CUSTOMER_MATCHING_CLASS} --driver-java-options "${SPARK_ECAT_PROP} -Dlog4j.configuration=log4j.properties -Dcustomer_matching.lookup_db=${LookupDb} -Dcustomer_matching.work_db=${work_db} -Dspark.coalesce_partitions=$CoalescePartitions -Dcustomer_matching.temp_result_area=/tmp/customer_matching/$FILE_PROCESSING/" $CUSTOMER_MATCHING_JAR_PATH \ "$@" >> ${mobule_out_file} 2>&1

exit_code=$?

fail_on_error=$BOOLEAN_TRUE

success_message="Successfully executed ${MODULE_NAME} spark script "

failure_message="Spark script ${MODULE_NAME} failed"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

hadoop fs -cat /apps/cdw/outgoing/exception_list/customerExceptionList/ExceptionList-$current_dt/* > /var/tmp/customerExceptionList.txt
if [ $? -eq 0 ]
then
    echo "HDFS Path created and fetched today's Exceptions list for customer."
else
    echo "HDFS path not exists"
fi


customerExceptionListCount=`cat /var/tmp/customerExceptionList.txt|wc -l`


hadoop fs -cat /apps/cdw/outgoing/exception_list/tlogExceptionList/ExceptionList-$current_dt/* > /var/tmp/tlogExceptionList.txt

if [ $? -eq 0 ]
then
    echo "HDFS Path created and fetched today's Exceptions list for Tlog."
else
    echo "HDFS path not exists"
fi

tlogExceptionListCount=`cat /var/tmp/tlogExceptionList.txt|wc -l`

hadoop fs -cat /apps/cdw/outgoing/exception_list/bpmemberExceptionList/ExceptionList-$current_dt/* > /var/tmp/bpmemberExceptionList.txt

if [ $? -eq 0 ]
then
    echo "HDFS Path created and fetched today's Exceptions list for BP Member."
else
    echo "HDFS path not exists"
fi

bpmemberExceptionListCount=`cat /var/tmp/bpmemberExceptionList.txt|wc -l`


if [ ${child_module} = "work_tlog_customer_cm" ];then
{

if [ $customerExceptionListCount -ne 0 ] || [ $tlogExceptionListCount -ne 0 ]
then
echo "This is to notify you that attached records rejected while loading customer_matching for ${MODULE_NAME} Path : $module_err_file and Error record count : ${customerExceptionListCount} AND ${tlogExceptionListCount}"| mailx -r "Express CDW Alerts <expresscdwalerts@express.com>" -s "Records moved to hdfs location while loading ${child_module}" -a ${TLOG_E_LIST} -a ${CUSTOMER_E_LIST} ${EMAIL_GROUP}
fi

}
elif [ ${child_module} = "work_bp_member_cm" ]
then
{

if [ $bpmemberExceptionListCount -ne 0 ]
then
echo "This is to notify you that attached records rejected while loading customer_matching for ${MODULE_NAME} Path : $module_err_file and Error record count : ${bpmemberExceptionListCount}"| mailx -r "Express CDW Alerts <expresscdwalerts@express.com>" -s "Records moved to hdfs location while loading ${child_module}" -a ${BP_MEMBER_E_LIST} ${EMAIL_GROUP} 
fi


}
fi


#exit_code=$?

#fail_on_error=$BOOLEAN_TRUE

#success_message="Successfully executed ${MODULE_NAME} spark script "

#failure_message="Spark script ${MODULE_NAME} failed"

#fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}


# Function to load kpi fact and aggregate tables
# Takes 11 arguments in below order
# current_batch_id, previous_batch_id,business_date,LOOKUP_DB,SOURCE_DB,TARGET_DB,COALESCE_PARTITIONS,MODULE_HOME_FL,CHILD_MODULE,FACT_LOAD_JAR_PATH

function fn_execute_KpiLoadSpark(){

BATCH_ID="$1"
PREVIOUS_BATCH_ID="$2"
BUSINESS_DATE="$3"
LOOKUP_DB="$4"
WORK_DB="$5"
BACKUP_DB="$6"
COALESCE_PARTITIONS="$7"
MODULE_HOME_FL="$8"
CHILD_MODULE="$9"
FACT_LOAD_JAR_PATH="${10}"

if [ -z ${FACT_LOAD_JAR_PATH} ]
then
FactLoadJarPath="/apps/appl/subject_areas/customer/customer_project/lib/customer-kpi-1.0.jar"
else
FactLoadJarPath=${FACT_LOAD_JAR_PATH}
fi

fn_get_current_batch_id
fn_get_previous_batch_id

fn_get_module_log_dir_and_file

module_type="spark"

module_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"

fn_log_info "Out file : ${module_out_file}"

if [  -f "${MODULE_HOME_FL}/etc/module.env.properties" ];
then
. ${MODULE_HOME_FL}/etc/module.env.properties
fi

if [  -f "${MODULE_HOME_FL}/${CHILD_MODULE}/etc/${CHILD_MODULE}.module.spark.properties" ];
then
. ${MODULE_HOME_FL}/${CHILD_MODULE}/etc/${CHILD_MODULE}.module.spark.properties
fi
get_spark_property_file

spark-submit --master ${SPARK_MASTER} --deploy-mode ${SPARK_DEPLOY_MODE} --files ${SPARK_EXTRA_FILES} --jars ${SPARK_EXTRA_JARS} --class ${FACT_LOAD_CLASS} --properties-file ${SPARK_PROP_FILE} --driver-java-options "${SPARK_ECAT_PROP} -Dlog4j.configuration=log4j.properties -Dcdw_config.gold_db=${LOOKUP_DB} -Dcdw_config.work_db=${WORK_DB} -Dcdw_config.backup_db=${BACKUP_DB} -Dspark.coalesce_partitions=${COALESCE_PARTITIONS}" $FactLoadJarPath \ "$@" >> ${module_out_file} 2>&1


exit_code=$?

fail_on_error=$BOOLEAN_TRUE

success_message="Successfully executed ${MODULE_NAME} spark script "

failure_message="Spark script ${MODULE_NAME} failed"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"
}





# Function To Load Fact tables and Dimention Tables
# Takes 7 Arguments in below order
# LOOKUP_DB,SOURCE_DB,TARGET_DB,COALESCE_PARTITIONS,MODULE_HOME_FL,CHILD_MODULE,FACT_LOAD_JAR_PATH

function fn_execute_DimFactLoadSpark(){
LOOKUP_DB="$1"
WORK_DB="$2"
BACKUP_DB="$3"
COALESCE_PARTITIONS="$4"
MODULE_HOME_FL="$5"
CHILD_MODULE="$6"
FACT_LOAD_JAR_PATH="$7"

if [ -z ${FACT_LOAD_JAR_PATH} ]
then
FactLoadJarPath="/apps/appl/subject_areas/customer/customer_project/lib/cdw-processing-"$(fn_get_version)".jar"
else
FactLoadJarPath=${FACT_LOAD_JAR_PATH}
fi

fn_get_current_batch_id

fn_get_module_log_dir_and_file

module_type="spark"

module_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"

fn_log_info "Out file : ${module_out_file}"

if [  -f "${MODULE_HOME_FL}/etc/module.env.properties" ];
then
. ${MODULE_HOME_FL}/etc/module.env.properties
fi

if [  -f "${MODULE_HOME_FL}/${CHILD_MODULE}/etc/${CHILD_MODULE}.module.spark.properties" ];
then
. ${MODULE_HOME_FL}/${CHILD_MODULE}/etc/${CHILD_MODULE}.module.spark.properties
fi
get_spark_property_file

spark-submit --master ${SPARK_MASTER} --deploy-mode ${SPARK_DEPLOY_MODE} --files ${SPARK_EXTRA_FILES} --jars ${SPARK_EXTRA_JARS} --class ${FACT_LOAD_CLASS} --properties-file ${SPARK_PROP_FILE} --driver-java-options "${SPARK_ECAT_PROP} -Dlog4j.configuration=log4j.properties -Dcdw_config.gold_db=${LOOKUP_DB} -Dcdw_config.work_db=${WORK_DB} -Dcdw_config.backup_db=${BACKUP_DB} -Dspark.coalesce_partitions=${COALESCE_PARTITIONS}" $FactLoadJarPath ${BATCH_ID} \ "$@" >> ${module_out_file} 2>&1

exit_code=$?

fail_on_error=$BOOLEAN_TRUE

success_message="Successfully executed ${MODULE_NAME} spark script "

failure_message="Spark script ${MODULE_NAME} failed"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"
}


# Function To Load Fact Card History table Table
# Takes 8 Arguments in below order
# LOOKUP_DB,SOURCE_DB,TARGET_DB,COALESCE_PARTITIONS,MODULE_HOME_FL,CHILD_MODULE,FACT_FILE,FACT_LOAD_JAR_PATH

function fn_execute_FactCardHistSpark(){
LOOKUP_DB="$1"
WORK_DB="$2"
BACKUP_DB="$3"
COALESCE_PARTITIONS="$4"
MODULE_HOME_FCL="$5"
CHILD_MODULE="$6"
FACT_FILE="$7"
FACT_LOAD_JAR_PATH="$8"



if [ -z ${FACT_LOAD_JAR_PATH} ]
then
FactLoadJarPath="/apps/appl/subject_areas/customer/customer_project/lib/cdw-processing-"$(fn_get_version)".jar"
else
FactLoadJarPath=${FACT_LOAD_JAR_PATH}
fi

fn_get_current_batch_id

fn_get_module_log_dir_and_file

module_type="spark"


module_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"


fn_log_info "Out file : ${module_out_file}"

if [  -f "${MODULE_HOME_FCL}/etc/module.env.properties" ];
then
. ${MODULE_HOME_FCL}/etc/module.env.properties
fi

if [  -f "${MODULE_HOME_FCL}/${CHILD_MODULE}/etc/${FACT_FILE}.module.spark.properties" ];
then
. ${MODULE_HOME_FCL}/${CHILD_MODULE}/etc/${FACT_FILE}.module.spark.properties
fi

get_spark_property_file

spark-submit --master ${SPARK_MASTER} --deploy-mode ${SPARK_DEPLOY_MODE} --files ${SPARK_EXTRA_FILES} --jars ${SPARK_EXTRA_JARS} --properties-file ${SPARK_PROP_FILE} --class ${FACT_LOAD_CLASS} --driver-java-options "${SPARK_ECAT_PROP} -Dlog4j.configuration=log4j.properties -Dcdw_config.gold_db=${LOOKUP_DB} -Dcdw_config.work_db=${WORK_DB} -Dcdw_config.backup_db=${BACKUP_DB} -Dspark.coalesce_partitions=${COALESCE_PARTITIONS}" $FactLoadJarPath ${BATCH_ID} \ "$@" >> ${module_out_file} 2>&1

exit_code=$?

fail_on_error=$BOOLEAN_TRUE

success_message="Successfully executed ${MODULE_NAME} spark script "

failure_message="Spark script ${MODULE_NAME} failed"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

function fn_execute_SparkSubmit(){
    PROCESS_FILE="$1"
    ENRICHORTRANFOEMFILE_CLASS="$2"
    ENRICHORTRANFOEMFILE_JAR="$3"
    PROCESSFILE_CLASS="$4"
    PROCESSFILE_JAR_PATH="$5"

    fn_get_module_log_dir_and_file
    module_type="spark"
    mobule_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"
    fn_log_info "Out file : ${mobule_out_file}"
    get_spark_property_file
   
    if [ $PROCESS_FILE == "dedup" ]
    then
    spark-submit --master ${SPARK_MASTER}  --deploy-mode ${SPARK_DEPLOY_MODE} --files ${SPARK_EXTRA_FILES} --jars ${SPARK_EXTRA_JARS} --properties-file ${SPARK_PROP_FILE} --class ${PROCESSFILE_CLASS} --driver-java-options "${SPARK_ECAT_PROP} -Dlog4j.configuration=log4j.properties  -Dcdw_config.work_db=${HIVE_DATABASE_NAME_WORK} -Dcdw_config.backup_db=${HIVE_DATABASE_NAME_BACKUP}  -Dspark.coalesce_partitions=${COALESCE_PARTITIONS}" ${PROCESSFILE_JAR_PATH} ${BATCH_ID} ${PROCESS_FILE} \ "$@" >> ${mobule_out_file} 2>&1

	exit_code=$?

    fail_on_error=$BOOLEAN_TRUE
    success_message="Successfully executed ${MODULE_NAME} scd spark script"
    failure_message="Spark script ${MODULE_NAME} scd failed"
    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"
    

	else
	echo "SPARK working -----------------------${PROCESSFILE_JAR_PATH} ${ENRICHORTRANFOEMFILE_JAR}  ${PROCESS_FILE}"
    spark-submit --master ${SPARK_MASTER}  --deploy-mode ${SPARK_DEPLOY_MODE} --files ${SPARK_EXTRA_FILES} --jars ${SPARK_EXTRA_JARS} --properties-file ${SPARK_PROP_FILE} --class ${ENRICHORTRANFOEMFILE_CLASS} --driver-java-options "${SPARK_ECAT_PROP} -Dlog4j.configuration=log4j.properties  -Dcdw_config.work_db=${HIVE_DATABASE_NAME_WORK} -Dcdw_config.look_up=${HIVE_DATABASE_NAME_GOLD} -Dcdw_config.backup_db=${HIVE_DATABASE_NAME_BACKUP}  -Dspark.coalesce_partitions=${COALESCE_PARTITIONS} " ${ENRICHORTRANFOEMFILE_JAR} ${PROCESS_FILE} \ "$@" >> ${mobule_out_file} 2>&1
    
    exit_code=$?    
    fail_on_error=$BOOLEAN_TRUE
	if [ $PROCESS_FILE != "enrich" ]
    then
        success_message="Successfully executed ${MODULE_NAME} transform spark script"
        failure_message="Spark script ${MODULE_NAME} transform failed"
    elif [ $PROCESS_FILE == "enrich" ]
    then 
        success_message="Successfully executed ${MODULE_NAME} enrich spark script"
        failure_message="Spark script ${MODULE_NAME} enrich failed"
    fi      
    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"
    
set -f
PROCESSFILE_CLASS=(${PROCESSFILE_CLASS//,/ })

for i in "${!PROCESSFILE_CLASS[@]}"
do
    get_spark_property_file
    
    echo -e "------------------->spark-submit --master ${SPARK_MASTER}  --deploy-mode ${SPARK_DEPLOY_MODE} --files ${SPARK_EXTRA_FILES} --jars ${SPARK_EXTRA_JARS} --properties-file ${SPARK_PROP_FILE} --class ${PROCESSFILE_CLASS[i]} --driver-java-options \"${SPARK_ECAT_PROP} -Dlog4j.configuration=log4j.properties  -Dcdw_config.work_db=${HIVE_DATABASE_NAME_WORK} -Dcdw_config.backup_db=${HIVE_DATABASE_NAME_BACKUP} -Dspark.coalesce_partitions=${COALESCE_PARTITIONS}\" ${PROCESSFILE_JAR_PATH} ${BATCH_ID} ${PROCESS_FILE} \"$@\" >> ${mobule_out_file} 2>&1<---------------"
    
    spark-submit --master ${SPARK_MASTER}  --deploy-mode ${SPARK_DEPLOY_MODE} --files ${SPARK_EXTRA_FILES} --jars ${SPARK_EXTRA_JARS} --properties-file ${SPARK_PROP_FILE} --class ${PROCESSFILE_CLASS[i]} --driver-java-options "${SPARK_ECAT_PROP} -Dlog4j.configuration=log4j.properties  -Dcdw_config.work_db=${HIVE_DATABASE_NAME_WORK} -Dcdw_config.backup_db=${HIVE_DATABASE_NAME_BACKUP} -Dspark.coalesce_partitions=${COALESCE_PARTITIONS}" ${PROCESSFILE_JAR_PATH} ${BATCH_ID} ${PROCESS_FILE} \ "$@" >> ${mobule_out_file} 2>&1
   
    exit_code=$?
    fail_on_error=$BOOLEAN_TRUE
    success_message="Successfully executed ${MODULE_NAME} scd spark script"
    failure_message="Spark script ${MODULE_NAME} scd failed"
    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"   
done


    #exit_code=$?
    #fail_on_error=$BOOLEAN_TRUE
    #success_message="Successfully executed ${MODULE_NAME} scd spark script"
    #failure_message="Spark script ${MODULE_NAME} scd failed"
    #fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"
   
    fi
}

function fn_submit_spark(){
    PROCESSFILE_CLASS=$1
    PROCESSFILE_JAR_PATH=$2
    SPARK_ARGS=$3
    SYS_PROP=$4

 	ARGS="--batch_id ${BATCH_ID} ${SPARK_ARGS}"
 	SYSTEM_PROP="${SPARK_ECAT_PROP} -Dlog4j.configuration=log4j.properties  -Dcdw_config.work_db=${HIVE_DATABASE_NAME_WORK} -Dcdw_config.look_up=${HIVE_DATABASE_NAME_GOLD} -Dcdw_config.backup_db=${HIVE_DATABASE_NAME_BACKUP}  -Dspark.coalesce_partitions=${COALESCE_PARTITIONS} ${SYS_PROP}"

    fn_get_module_log_dir_and_file
    module_type="spark"
    module_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"
    fn_log_info "Out file : ${module_out_file}"
    get_spark_property_file

    echo "spark-submit --master ${SPARK_MASTER}  --deploy-mode ${SPARK_DEPLOY_MODE} --files ${SPARK_EXTRA_FILES} --jars ${SPARK_EXTRA_JARS} --properties-file ${SPARK_PROP_FILE} --class ${PROCESSFILE_CLASS} --driver-java-options \"${SYSTEM_PROP}\" ${PROCESSFILE_JAR_PATH} ${ARGS}"

 	spark-submit --master ${SPARK_MASTER}  --deploy-mode ${SPARK_DEPLOY_MODE} --files ${SPARK_EXTRA_FILES} --jars ${SPARK_EXTRA_JARS} --properties-file ${SPARK_PROP_FILE} --class ${PROCESSFILE_CLASS} --driver-java-options "${SYSTEM_PROP}" ${PROCESSFILE_JAR_PATH} ${ARGS} >> ${module_out_file} 2>&1

    exit_code=$?
    success_message="Successfully executed ${MODULE_NAME} spark script"
    failure_message="Spark script ${MODULE_NAME} failed"
    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"
}

function fn_execute_DimFactDedupLoadSpark(){
LOOKUP_DB="$1"
WORK_DB="$2"
TABLE_NAME="$3"
LOAD_TYPE="$4"
MODULE_HOME_FL="$5"
CHILD_MODULE="$6"
DIMFACT_DEDUP_LOAD_JAR_PATH="$7"

if [ -z ${DIMFACT_DEDUP_LOAD_JAR_PATH} ]
then
DimFactDedupLoadJarPath="/apps/appl/subject_areas/customer/customer_project/lib/customer-dedup-"$(fn_get_version)".jar"
else
DimFactDedupLoadJarPath=${DIMFACT_DEDUP_LOAD_JAR_PATH}
fi

fn_get_current_batch_id

fn_get_module_log_dir_and_file

module_type="spark"

module_out_file="${LOG_DIR_MODULE}/${module_type}.${TABLE_NAME}.${LOG_FILE_NAME_MODULE}.out"

fn_log_info "Out file : ${module_out_file}"

if [  -f "${MODULE_HOME_FL}/etc/module.env.properties" ];
then
. ${MODULE_HOME_FL}/etc/module.env.properties
fi

if [  -f "${MODULE_HOME_FL}/${CHILD_MODULE}/etc/${CHILD_MODULE}.module.spark.properties" ];
then
. ${MODULE_HOME_FL}/${CHILD_MODULE}/etc/${CHILD_MODULE}.module.spark.properties
fi
get_spark_property_file
spark-submit --master ${SPARK_MASTER} --deploy-mode ${SPARK_DEPLOY_MODE} --class ${FACT_DIM_DEDUP_LOAD_CLASS} --files ${SPARK_EXTRA_FILES} --jars ${SPARK_EXTRA_JARS} --properties-file ${SPARK_PROP_FILE} --driver-java-options "${SPARK_ECAT_PROP} -Dlog4j.configuration=log4j.properties -Dcdw_config.gold_db=${LOOKUP_DB} -Dcdw_config.work_db=${WORK_DB} -Dcdw_config.backup_db=${BACKUP_DB}" $DimFactDedupLoadJarPath ${TABLE_NAME} ${LOAD_TYPE} ${BATCH_ID} \ "$@" >> ${module_out_file} 2>&1

exit_code=$?

fail_on_error=$BOOLEAN_TRUE

success_message="Successfully executed ${MODULE_NAME} spark script "

failure_message="Spark script ${MODULE_NAME} failed"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"
}


################################################################################
#                                     End                                      #
################################################################################

# Function To Load Fact tables and Dimention Tables
# Takes 7 Arguments in below order
# COALESCE_PARTITIONS,MODULE_HOME_FL,CHILD_MODULE,FACT_LOAD_JAR_PATH
function fn_execute_DimLoadSpark(){
LOOKUP_DB="$1"
WORK_DB="$2"
BACKUP_DB="$3"
COALESCE_PARTITIONS="$4"
MODULE_HOME_FL="$5"
CHILD_MODULE="$6"
DIM_LOAD_JAR_PATH="$7"
PROCESS_FILE="$8"

if [ -z ${DIM_LOAD_JAR_PATH} ]
then
DimLoadJarPath="/apps/appl/subject_areas/customer/customer_project/lib/dim-fact-load-"$(fn_get_version)".jar"
else
DimLoadJarPath=${DIM_LOAD_JAR_PATH}
fi

fn_get_current_batch_id

fn_get_module_log_dir_and_file

module_type="spark"

module_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"

fn_log_info "Out file : ${module_out_file}"

if [  -f "${MODULE_HOME_FL}/etc/module.env.properties" ];
then
. ${MODULE_HOME_FL}/etc/module.env.properties
fi

if [  -f "${MODULE_HOME_FL}/${CHILD_MODULE}/etc/${CHILD_MODULE}.module.spark.properties" ];
then
. ${MODULE_HOME_FL}/${CHILD_MODULE}/etc/${CHILD_MODULE}.module.spark.properties
fi
get_spark_property_file
spark-submit --master ${SPARK_MASTER} --deploy-mode ${SPARK_DEPLOY_MODE} --class ${DIM_LOAD_CLASS} --files ${SPARK_EXTRA_FILES} --jars ${SPARK_EXTRA_JARS} --properties-file ${SPARK_PROP_FILE} --driver-java-options "${SPARK_ECAT_PROP} -Dlog4j.configuration=log4j.properties -Dcdw_config.gold_db=${LOOKUP_DB} -Dcdw_config.work_db=${WORK_DB} -Dcdw_config.backup_db=${BACKUP_DB} -Dspark.coalesce_partitions=${COALESCE_PARTITIONS}" $DimLoadJarPath ${BATCH_ID} ${PROCESS_FILE} \ "$@" >> ${module_out_file} 2>&1

exit_code=$?

if [ ${exit_code} = 2 ] && [ ${CHILD_MODULE} = "dim_scoring_model" ]

then

echo "Exiting the spark script as the dim_scoring_model process should occur on the first Wednesday of each month"

exit 0

else

fail_on_error=$BOOLEAN_TRUE

success_message="Successfully executed ${MODULE_NAME} spark script "

failure_message="Spark script ${MODULE_NAME} failed"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

fi
}
#
#


#
#function to run Customer Deduplication Process
#this function will load work.dim_member and gold.fact_dedupe_info
#
#
function fn_execute_CustomerDeduplicationSpark(){

LookupDb="$1"
work_db="$2"
CoalescePartitions="$3"
#MODULE_HOME_DEDUP="$4"
#child_module="$5"
MODULE_HOME_DEDUP=${MODULE_HOME}
child_module=${CHILD_MODULE}

echo ${MODULE_HOME_DEDUP}
echo ${child_module}

fn_get_current_batch_id

fn_get_module_log_dir_and_file

module_type="spark"


module_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"

fn_log_info "Out file : ${mobule_out_file}"
if [  -f "${MODULE_HOME_DEDUP}/etc/module.env.properties" ];
then
. ${MODULE_HOME_DEDUP}/etc/module.env.properties
fi

if [  -f "${MODULE_HOME_DEDUP}/etc/${child_module}.module.env.properties" ];
then
. ${MODULE_HOME_DEDUP}/etc/${child_module}.module.env.properties
fi

get_spark_property_file

spark-submit  --master ${SPARK_MASTER}  --deploy-mode ${SPARK_DEPLOY_MODE}  --files ${SPARK_EXTRA_FILES} --jars ${SPARK_EXTRA_JARS} --properties-file ${SPARK_PROP_FILE} --class ${CUSTOMER_DEDUP_CLASS} --driver-java-options "${SPARK_ECAT_PROP} -Dlog4j.configuration=log4j.properties -Dcdw_config.gold_db=${LOOKUP_DB} -Dcdw_config.work_db=${WORK_DB} -Dcustomer_deduplication.temp_result_area=/tmp/customer_deduplication/$FILE_PROCESSING/" $CUSTOMER_DEDUPLICATION_JAR_PATH ${BATCH_ID} \ "$@" >> ${module_out_file} 2>&1

##this code to be inserted after work_db
##-Dspark.coalesce_partitions=$CoalescePartitions

exit_code=$?

fail_on_error=$BOOLEAN_TRUE

success_message="Successfully executed ${MODULE_NAME} spark script "

failure_message="Spark script ${MODULE_NAME} failed"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

#function for history load
function fn_execute_historyLoadSparkSubmit(){

PROCESSFILE_JAR_PATH="$1"
PROCESSFILE_CLASS="$2"
FROM_DATE="$3"
TO_DATE="$4"

fn_get_current_batch_id
fn_get_module_log_dir_and_file

module_type="spark"
mobule_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"
fn_log_info "Out file : ${mobule_out_file}"
get_spark_property_file
spark-submit --master ${SPARK_MASTER}  --deploy-mode ${SPARK_DEPLOY_MODE} --files ${SPARK_EXTRA_FILES} --jars ${SPARK_EXTRA_JARS} --properties-file ${SPARK_PROP_FILE} --class ${PROCESSFILE_CLASS} --driver-java-options "${SPARK_ECAT_PROP} -Dlog4j.configuration=log4j.properties  -Dcdw_config.gold_db=${HIVE_DATABASE_NAME_GOLD}" ${PROCESSFILE_JAR_PATH} ${HIVE_DATABASE_NAME_CDH}  ${BATCH_ID} ${FROM_DATE} ${TO_DATE}\ "$@" >> ${mobule_out_file} 2>&1

exit_code=$?

fail_on_error=$BOOLEAN_TRUE
success_message="Successfully executed ${MODULE_NAME} history load spark script"
failure_message="Spark script ${MODULE_NAME} history load failed"
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

function fn_cleanse(){

fn_get_current_batch_id
fn_get_module_log_dir_and_file

module_type="clean"
module_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"

fn_log_info "Out file : ${module_out_file}"

{
if [ -d "$GET_TEMP_DIR/${GET_PROCESSED_FOLDER}/$(date +"%Y%m%d")" ]
        then
        rm -rf ${GET_TEMP_DIR}/${GET_PROCESSED_FOLDER}/$(date +"%Y%m%d")/*
        cd ${GET_TEMP_DIR}/${GET_PROCESSED_FOLDER}/$(date +"%Y%m%d")
else
        mkdir -p ${GET_TEMP_DIR}/${GET_PROCESSED_FOLDER}/$(date +"%Y%m%d")
fi
        cp ${GET_TEMP_DIR}/${GET_UNPROCESSED_FOLDER}/$(date +"%Y%m%d")/* ${GET_TEMP_DIR}/${GET_PROCESSED_FOLDER}/$(date +"%Y%m%d")/
                cd ${GET_TEMP_DIR}/${GET_PROCESSED_FOLDER}/$(date +"%Y%m%d")/
for j in *;
do
        sed -i 's/"//g' "$j"
                if [ $? -ne 0 ]
                        then
                                echo "[ERROR]: File Cleaning Activity Failed for File : $j"
                                exit 1
                        else
                                echo "[INFO]:  File Cleaning Activity Successful for File : $j"
                fi
done

# Process to remove header-if required
if [ ${HEADER} == 'Y' ]
then
cd ${GET_TEMP_DIR}/${GET_PROCESSED_FOLDER}/$(date +"%Y%m%d")/
for j in *;
do
        sed -i "1d" "$j"
                if [ $? -ne 0 ]
                        then
                                echo "[ERROR]: Failed To Remove File Header For File : $j"
                                exit 1
                        else
                                echo "[INFO]:  File Header Removed Successfully for File : $j"
                fi
done
fi
} >> ${module_out_file} 2>&1
exit_code=$?
fail_on_error=$BOOLEAN_TRUE

success_message="Successfully executed ${MODULE_NAME} Shell script "

failure_message="Shell script ${MODULE_NAME} failed"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

function fn_getfile(){

fn_get_current_batch_id
fn_get_module_log_dir_and_file

module_type="get"
module_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"

fn_log_info "Out file : ${module_out_file}"

{
if [ -d "${GET_TEMP_DIR}/${GET_UNPROCESSED_FOLDER}/$(date +"%Y%m%d")" ]
        then
        rm -rf ${GET_TEMP_DIR}/${GET_UNPROCESSED_FOLDER}/$(date +"%Y%m%d")/*
        #sftp -o IdentityFile=${PRIVATE_KEY_PATH} ${SFTP_USER}@${SFTP_HOST} <<EOF
        #get ${GET_SRC_FILE_DIR}/${GET_PATTERN}* ${GET_TEMP_DIR}/${GET_UNPROCESSED_FOLDER}/$(date +"%Y%m%d")/
#EOF

else
        mkdir -p ${GET_TEMP_DIR}/${GET_UNPROCESSED_FOLDER}/$(date +"%Y%m%d")
        #sftp -o IdentityFile=${PRIVATE_KEY_PATH} ${SFTP_USER}@${SFTP_HOST} <<EOF
        #get ${GET_SRC_FILE_DIR}/${GET_PATTERN}* ${GET_TEMP_DIR}/${GET_UNPROCESSED_FOLDER}/$(date +"%Y%m%d")/
#EOF
fi

cp ${GET_SRC_FILE_DIR}/${GET_PATTERN}* ${GET_TEMP_DIR}/${GET_UNPROCESSED_FOLDER}/$(date +"%Y%m%d")/

} >> ${module_out_file} 2>&1

exit_code=$?
fail_on_error=$BOOLEAN_TRUE

success_message="Successfully executed ${MODULE_NAME} Shell script "

failure_message="Shell script ${MODULE_NAME} failed"

fn_handle_exit_cuccess_message="File Transfer To SFTP Location Successful for All Files dated ${NEXT_DATE}"success_message="File Transfer To SFTP Location Successful for All Files dated ${NEXT_DATE}":/de "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"


}

function fn_putfile(){

fn_get_current_batch_id
fn_get_module_log_dir_and_file

module_type="put"
module_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"

fn_log_info "Out file : ${module_out_file}"

{
sftp ${SFTP_USER}@${SFTP_HOST} <<EOF
cd ${EXPRESS_TGT_DIR}
put ${PUT_TEMP_DIR}/${PUT_PROCESSED_FOLDER}/$BATCH_ID/*.PGP
put ${PUT_TEMP_DIR}/${PUT_PROCESSED_FOLDER}/$BATCH_ID/*.trigger
EOF

} >> ${module_out_file} 2>&1
                
                exit_code=$?
                fail_on_error=$BOOLEAN_TRUE

                success_message="File Transfer To SFTP Location Successful for All Files dated ${NEXT_DATE}"

                failure_message="File Transfer To SFTP Location Failed for All Files dated ${NEXT_DATE}"

                fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"
                              
       }

function fn_encrypt(){

fn_get_current_batch_id
fn_get_module_log_dir_and_file

module_type="encrypt"
module_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"

fn_log_info "Out file : ${module_out_file}"

{
    if gpg --list-keys;
        then
                gpg --import /hadoop/tools/keys/*
                        gpg --import /hadoop/tools/keys/brierley_encrypt/*
                                gpg --import /hadoop/tools/keys/brierley_decrypt/*
                                        cd ${ENCRYPT_FOLDER}
                                                echo "$FILE_NAME is the encryption source directory"
                                                        for file in ${ENCRYPT_FOLDER}*.TXT; do
                                                                    echo "encrypting $file"
                                                                                    #echo "gpg --trust-model always --encrypt --recipient ${RECIPIENT} ${file}"
                                                                                                    #gpg --trust-model always --encrypt --recipient "${RECIPIENT}" "${file}"
                                                                                                                    gpg --trust-model always --recipient "${RECIPIENT}" --output ${file}.PGP --encrypt ${file}
                                                                                                                            done
                                                                                                                                else
                                                                                                                                    echo "${FILE_NAME} is not encrypted"
                                                                                                                                        fi
                                                                                                                                                
                                                                                                                                                } >> ${module_out_file} 2>&1

                                                                                                                                                exit_code=$?
                                                                                                                                                fail_on_error=$BOOLEAN_TRUE

                                                                                                                                                success_message="Successfully executed ${MODULE_NAME} Shell script "

                                                                                                                                                failure_message="Shell script ${MODULE_NAME} failed"

                                                                                                                                                fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

                                                                                                                                                }

function fn_decrypt(){

fn_get_current_batch_id
fn_get_module_log_dir_and_file

module_type="decrypt"
module_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"

fn_log_info "Out file : ${module_out_file}"

{
        if gpg --list-keys;
                        then
                                            gpg --import /hadoop/tools/keys/*
                                                                    if [[ -d $FILE_DECRYPT ]]; then
                                                                                                    echo "$FILE_DECRYPT is a directory"
                                                                                                                                            for file in ${FILE_DECRYPT}*; do
                                                                                                                                                                                            echo "decrypting $file"
                                                                                                                                                                                                                                            gpg --batch --yes --passphrase="${PASSPHRASE}" -d  ${file}
                                                                                                                                                                                                                                                                                    done
                                                                                                                                                                                                                                                                                                            else
                                                                                                                                                                                                                                                                                                                                            gpg --batch --yes --passphrase="${PASSPHRASE}" -d  ${FILE_DECRYPT}
                                                                                                                                                                                                                                                                                                                                                                            echo "${FILE_DECRYPT} Decrypted Successfully"
                                                                                                                                                                                                                                                                                                                                                                                                    fi
                                                                                                                                                                                                                                                                                                                                                                                                            else
                                                                                                                                                                                                                                                                                                                                                                                                                                    echo "${FILE_DECRYPT} is not decrypted"
                                                                                                                                                                                                                                                                                                                                                                                                                                            fi

                                                                                                                                                                                                                                                                                                                                                                                                                                            } >> ${module_out_file} 2>&1
                                                                                                                                                                                                                                                                                                                                                                                                                                                    
                                                                                                                                                                                                                                                                                                                                                                                                                                                    exit_code=$?
                                                                                                                                                                                                                                                                                                                                                                                                                                                    fail_on_error=$BOOLEAN_TRUE

                                                                                                                                                                                                                                                                                                                                                                                                                                                    success_message="Successfully executed ${MODULE_NAME} Shell script "

                                                                                                                                                                                                                                                                                                                                                                                                                                                    failure_message="Shell script ${MODULE_NAME} failed"

                                                                                                                                                                                                                                                                                                                                                                                                                                                    fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"
                                                                                                                                                                                                                                                                                                                                                                                                                                                            
                                                                                                                                                                                                                                                                                                                                                                                                                                                            }

function fn_rename_hdfs_files(){

folder="$1"

file_suffix="$2"

assert_variable_is_set "folder" "${folder}"

assert_variable_is_set "file_suffix" "${file_suffix}"

fn_log_info "Renaming all files in $folder with prefix: $file_suffix"

echo "java -cp :${HOME}/subject_areas/customer/customer_project/lib/dim-fact-load-"$(fn_get_version)".jar:${SPARK_LIB_PATH}:${HOME}/subject_areas/customer/etc/ com.express.util.HDFSRenameUtil $folder $file_suffix"

java -cp :${HOME}/subject_areas/customer/customer_project/lib/dim-fact-load-"$(fn_get_version)".jar:${SPARK_LIB_PATH}:${HOME}/subject_areas/customer/etc/ com.express.util.HDFSRenameUtil $folder $file_suffix

exit_code="$?"

fail_on_error=$BOOLEAN_TRUE

success_message="Successfully executed ${MODULE_NAME} Shell script "

failure_message="Shell script ${MODULE_NAME} failed"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}


function fn_get_files_from_sftp(){

fn_get_current_batch_id
fn_get_module_log_dir_and_file

module_type="get"
module_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"

fn_log_info "Out file : ${module_out_file}"

{

sftp ${SFTP_USER}@${SFTP_HOST} <<EOF
get ${SFTP_RC_IN_DIR}/${GET_PATTERN_RC}* ${LW_RC_TEMP_LOC}/
get ${SFTP_LW_IN_DIR}/${GET_PATTERN_LW}* ${LW_RC_TEMP_LOC}/
EOF

} >> ${module_out_file} 2>&1

exit_code=$?
fail_on_error=$BOOLEAN_TRUE

success_message="File Transfer From SFTP Location to Local temp location Successful"

failure_message="File Transfer From SFTP Location to Local temp location failed"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"


}



function fn_cleanse_files(){

fn_get_current_batch_id
fn_get_module_log_dir_and_file

module_type="clean"
module_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"

fn_log_info "Out file : ${module_out_file}"

{
if [ -d "${LW_RC_TEMP_LOC}" ]
then

cd ${LW_RC_TEMP_LOC}
else
echo "Files are not available location : ${LW_RC_TEMP_LOC} "
exit 1
fi

for j in *;
do
sed -i 's/"//g' "$j"
if [ $? -ne 0 ]
then
echo "[ERROR]: File Cleaning Activity Failed for File : $j"
exit 1
else
echo "[INFO]:  File Cleaning Activity Successful for File : $j"
fi
done

# Process to remove header-if required
if [ ${HEADER} == 'Y' ]
then
cd ${LW_RC_TEMP_LOC}/
for j in *;
do
sed -i "1d" "$j"
if [ $? -ne 0 ]
then
echo "[ERROR]: Failed To Remove File Header For File : $j"
exit 1
else
echo "[INFO]:  File Header Removed Successfully for File : $j"
fi
done
fi
} >> ${module_out_file} 2>&1
exit_code=$?
fail_on_error=$BOOLEAN_TRUE

success_message="Successfully executed File Cleaning Activity. "

failure_message="Shell script File Cleaning Activity failed"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}



function fn_put_files_to_sftp(){

fn_get_current_batch_id
fn_get_module_log_dir_and_file

module_type="put"
module_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"

fn_log_info "Out file : ${module_out_file}"


{
if [ -d "${LW_RC_TEMP_LOC}" ]
then

sftp ${SFTP_USER}@${SFTP_HOST} <<EOF
put ${LW_RC_TEMP_LOC}/${GET_PATTERN_LW}* ${SFTP_LW_OUT_DIR}/
put ${LW_RC_TEMP_LOC}/${GET_PATTERN_RC}* ${SFTP_RC_OUT_DIR}/
EOF
fi

} >> ${module_out_file} 2>&1

exit_code=$?
fail_on_error=$BOOLEAN_TRUE

success_message="File Transfer to SFTP Location Successful"

failure_message="File Transfer to SFTP Location Failed"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}




function fn_sftp_delete_pattern_files(){

path="$1"
pattern="$2"

fn_get_current_batch_id
fn_get_module_log_dir_and_file

module_type="rm"
module_out_file="${LOG_DIR_MODULE}/${module_type}.${LOG_FILE_NAME_MODULE}.out"

fn_log_info "Out file : ${module_out_file}"

{
sftp ${SFTP_USER}@${SFTP_HOST} <<EOF
rm ${path}/${pattern}*
EOF

} >> ${module_out_file} 2>&1

exit_code=$?
fail_on_error=$BOOLEAN_TRUE

success_message="Files removed From specified directory ${path} in SFTP Location Successful"

failure_message="Files removed From specified directory ${path} in SFTP Location failed"

fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"

}

function fn_get_version() {

    grep "git.build.version" /apps/appl/etc/version.properties|cut -d'=' -f2

    }

###################################################################################################################
function fn_copy_files_to_hdfs_segregate_txt_and_pgp(){

file_or_folder=$1

to_hdfs_directory=$2

to_hdfs_directory_with_batchid=$3


fail_on_error=$4



  assert_variable_is_set "file_or_folder" "${file_or_folder}"
    assert_variable_is_set "to_hdfs_directory" "${to_hdfs_directory}"


      hdfs dfs -copyFromLocal ${file_or_folder}/*.PGP* "${to_hdfs_directory}"
        exit_code=$?
          success_message="Successfully copied files from ${file_or_folder} to HDFS directory ${to_hdfs_directory}"
            failure_message="Failed to copy files from ${file_or_folder} to HDFS directory ${to_hdfs_directory}"
              fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"


                assert_variable_is_set "to_hdfs_directory with batchid" "${to_hdfs_directory_with_batchid}"
                  hdfs dfs -copyFromLocal ${file_or_folder}/*.TXT "${to_hdfs_directory_with_batchid}"
                    exit_code=$?
                      success_message="Successfully copied files from ${file_or_folder} to HDFS directory with batchid ${to_hdfs_directory_with_batchid}"
                        failure_message="Failed to copy files from ${file_or_folder} to HDFS directory with batchid ${to_hdfs_directory_with_batchid}"
                          fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${fail_on_error}"
                         # fi
                            }


################################################################################
#                                     End                                      #
################################################################################
