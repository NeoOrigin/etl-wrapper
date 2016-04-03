#!/bin/sh
#--------------------------------------------------------------------------------------------------------------------
# Script      : syncsort.sh
# Summary     : A generic script called by etlw to provide custom syncsort capability
# Details     : This script acts as a syncsort specific wrapper, intended to be called by etlw
#
# Parameters  : None
#
# To Do
# --------------
#
#
# Change History
# --------------
#
# Date         Author             Version    Description
# ------------ ------------------ ---------- ------------------------------------------------------------------------
# 20/02/2015   Philip Bowditch    1.0        Initial Version
#--------------------------------------------------------------------------------------------------------------------

#-- Main ------------------------------------------------------------------------------------------------------------

if [[ -z "${ETL_HOME}" ]]
then
    printf "%s %s\n" ETL_HOME "is not set, it should be set to the installation directory of etl-wrapper" >&2
    exit 1
fi

if [[ ! -d "${ETL_HOME}" && ! -L "${ETL_HOME}" ]]
then
    printf "%s %s\n" ETL_HOME "is not a valid directory, it should be set to the installation directory of etl-wrapper" >&2
    exit 1
fi

if [[ ! -f "${ETL_HOME}/lib/functions.ksh" && ! -L "${ETL_HOME}/lib/functions.ksh" ]]
then
    printf "%s %s\n" "ETL_HOME/lib/functions.ksh" "is not a valid script, does ETL_HOME point to a valid installation directory?" >&2
    exit 1
fi

if [[ ! -r "${ETL_HOME}/lib/functions.ksh" || ! -x "${ETL_HOME}/lib/functions.ksh" ]]
then
    printf "%s %s\n" "ETL_HOME/lib/functions.ksh" "could not be sourced, please check file permissions" >&2
    exit 1
fi

. "${ETL_HOME}/lib/functions.ksh"
if [[ ! -r "${ETL_HOME}/lib/functions.ksh" || ! -x "${ETL_HOME}/lib/functions.ksh" ]]
then
    printf "%s %s\n" "ETL_HOME/lib/functions.ksh" "could not be sourced, please check file permissions" >&2
    exit 1
fi


# These are temporary parameters used by this script, ensure they have no value
unset ETL_SS_HADOOP
unset ETL_SS_RECOVER_OPTIONS
unset TMP_PARAMETER_LIST
unset TMP_FORMAT

# Check if user wants us to pretty print the xml output
ETL_SS_PRETTY_PRINT=${ETL_SS_PRETTY_PRINT:-true}
ETL_SS_LOG_FORMAT==${ETL_SS_LOG_FORMAT:-XML}
ETL_SS_BASE_DATA_DIR=${ETL_SS_BASE_DATA_DIR:-"${ETL_BASE_DIR}/${ETL_BRANCH}"}


# Syncsort really supports 2 output formats, XML or text, however if xmllint is installed
# we can convert the xml to html
case "${ETL_SS_LOG_FORMAT}" in

    HTML ) TMP_FORMAT=XML
           ;;
    XML  ) TMP_FORMAT=XML
           ;;
    TEXT ) TMP_FORMAT=TEXT
           ;;
    *    ) ;;

esac

# Is this a syncsort hadoop job
if [[ "${ETL_SS_JOB_TYPE}" == "HADOOP" ]]
then
    ETL_SS_HADOOP="/HADOOP"
fi

# By default we do not compress workfiles unless we are using hadoop
if [[ -z "${ETL_SS_COMPRESS_WORKFILES}" ]]
then
    ETL_SS_COMPRESS_WORKFILES=OFF

    if [[ -n "${ETL_SS_HADOOP}" ]]
    then
        ETL_SS_COMPRESS_WORKFILES=ON
    fi
fi

# Build command line for parameters
if [[ "${#ETL_PARAMS[@]}" -gt 0 ]]
then
    TMP_PARAMETER_LIST="/EXPORT ${ETL_PARAMS[@]}"
fi

# Build command line for recoverability, this will eventually fail if ETL_SS_RECOVER_DIR is not set
if [[ "${ETL_RECOVER}" == "true" ]]
then
    ETL_SS_RECOVER_OPTIONS="ENABLERESTART RUNSTATEDIRECTORY '${ETL_SS_RECOVER_DIR}'"
fi



# If jobs are defined with relative paths we override the data directory path
export DMXDataDirectory="${ETL_SS_BASE_DATA_DIR}/${ETL_REL_PATH}/${ETL_PROJECT}"

# Add a test flag if one has been specified
if [[ -n "${ETL_SS_TEST_FLAG}" ]]
then
    DMXDataDirectory="${DMXDataDirectory}/${ETL_SS_TEST_FLAG}"
fi

cd "${DMXDataDirectory}"

RC=0


log_info "INITIALIZING" "Initializing project parameters"

# Project parameters might be saved with the project, allow them to be sourced on startup of the job
if [[ -f "${ETL_BASE_DIR}/${ETL_BRANCH}/${ETL_REL_PATH}/${ETL_PROJECT}/.project-parameters.ksh" ]]; then
    . ${ETL_BASE_DIR}/${ETL_BRANCH}/${ETL_REL_PATH}/${ETL_PROJECT}/.project-parameters.ksh
fi

# Dont log TOO much to screen, only log for first instance if running multiple jobs (not ideal)
log_info "ENVIRONMENT" "Listing environment variables"

printenv | env

log_info "DISKSPACE" "Listing available mount points"

df -a -PT

log_info "HOSTINFO" "Running Syncsort - getsysinfo"

# Output some syncsort specs about the host
getsysinfo

##
## Currently no way to validate a job other than run it, dmxdiff is a windows utility not unix
##

# Validate before running the job if required
#if [[ "${ETL_VALIDATION_LEVEL}" != IGNORE ]]
#then

#    log_info "VALIDATING" "Validating file..."

#    # Try validation, syncsort doesnt really validate so we try our best by using their utilities to query the files (hoping that they will fail on error)
#    case "${ETL_VALIDATION_LEVEL}" in

#        ABORT   ) dmxdiff /FORMAT TEXT "${ETL_BASE_DIR}/${ETL_BRANCH}/${ETL_REL_PATH}/${ETL_PROJECT}/${ETL_PROGRAM}" "~/a.dat" || exit 1
#                  ;;
#        WARNING ) dmxdiff /FORMAT TEXT "${ETL_BASE_DIR}/${ETL_BRANCH}/${ETL_REL_PATH}/${ETL_PROJECT}/${ETL_PROGRAM}" "~/a.dat"
#                  ;;

#    esac

#fi

# Start running the job
if [[ "$ETL_EXECUTE" == "true" ]]; then

    log_info "STARTING" "dmxjob /RUN '${ETL_BASE_DIR}/${ETL_BRANCH}/${ETL_REL_PATH}/${ETL_PROJECT}/${ETL_PROGRAM}' ${ETL_SS_RECOVER_OPTIONS} /LOG FORMAT '${TMP_FORMAT}' /COMPRESSWORKFILES '${ETL_SS_COMPRESS_WORKFILES}' ${ETL_SS_HADOOP} ${TMP_PARAMETER_LIST}"

    trap 'ERROR_CODE=$?; log_error "ENDED" "Failed, return code=$ERROR_CODE, elapsed $SECONDS seconds"; return $ERROR_CODE' INT HUP QUIT TERM

    # Required so we see the error code of dmxjob, not subsequent commands on the pipe
    set -o pipefail

    # Run the syncsort job, provide some pretty print capability of the xml if required (although this is dangerous as it might introduce its own errors e.g. if job is killed)
    dmxjob /RUN               "${ETL_BASE_DIR}/${ETL_BRANCH}/${ETL_REL_PATH}/${ETL_PROJECT}/${ETL_PROGRAM}" \
           ${ETL_SS_RECOVER_OPTIONS}                                                                        \
           /LOG FORMAT        "${TMP_FORMAT}"                                                               \
           /COMPRESSWORKFILES "${ETL_SS_COMPRESS_WORKFILES}"                                                \
           ${ETL_SS_HADOOP}                                                                                 \
           ${TMP_PARAMETER_LIST}     2>&1 |   if [[ "${ETL_SS_PRETTY_PRINT}" == "true" && "${ETL_SS_LOG_FORMAT}" == "XML"  ]]; then
                                                 xmllint --recover --format -
                                            elif [[ "${ETL_SS_LOG_FORMAT}" == "HTML" ]]; then
                                                 xmllint --recover --format --htmlout -
                                            else
                                                 cat
                                            fi

    RC=$?

else

    log_warning "SKIPPING" "dmxjob /RUN '${ETL_BASE_DIR}/${ETL_BRANCH}/${ETL_REL_PATH}/${ETL_PROJECT}/${ETL_PROGRAM}' ${ETL_SS_RECOVER_OPTIONS} /LOG FORMAT '${ETL_SS_LOG_FORMAT}' /COMPRESSWORKFILES '${ETL_SS_COMPRESS_WORKFILES}' ${ETL_SS_HADOOP} ${TMP_PARAMETER_LIST}"

fi

if [[ "$RC" -eq 0 ]]
then
    log_info  "ENDED" "Passed, return code=$RC, elapsed $SECONDS seconds"
else
    log_error "ENDED" "Failed, return code=$RC, elapsed $SECONDS seconds"
fi

exit $RC
