#!/usr/bin/env ksh
#--------------------------------------------------------------------------------------------------------------------
# Script      : functions.sh
# Summary     : A generic script to call ETL code
# Details     : This script acts as a wrapper for any scheduler such as tws or control-m to call any ETL code module,
#               including ab initio graphs, psets, simple ksh scripts etc.  Its intention is to provide a generic
#               technology neutral ETL abstraction Layer, reducing custom development when integrating and calling
#               ETL jobs via a scheduler, separating wrapper scripts from underlying ETL technology specifics such as
#               environment setup or post run activities etc but also provide standardised formatting for quicker
#               production defect resolution.
#
#               This script implements a number of customisation options through environment variables, command line
#               options as well as the introduction of override files (think custom .profile files).  These options
#               allow developers / administrators to set varied configuration options depending on the ETL job
#               being called e.g. redirect log output to different directories for different code branches,
#               executing modules under a different user id for different projects, turn off module execution or
#               perform enhanced validation for specific job names etc.
#
#               The exact order of precedence of these configuration options are as follows:
#                                                                                                                             _ _
#                     1. (e) - Environment variables                                                                           |
#                     2. (i) - Explicit script includes passed to the wrapper                                                  |
#                     3. (c) - Command line options                                                                            |
#                     4. (u) - User level override file              (${HOME}/.etl/etl_overrides.ksh)                          |
#                     5. (j) - User level Jobname override file      (${HOME}/.etl/etl_overrides.jobname.<jobname>.ksh)        |
#                     6. (p) - User level Project override file      (${HOME}/.etl/etl_overrides.project.<project>.ksh)        |
#                     7. (b) - User level Branch override file       (${HOME}/.etl/etl_overrides.branch.<branch>.ksh)          |
#                     8. (s) - User level System level override file (${HOME}/.etl/etl_overrides.system.<system>.ksh)          |
#                     9. (G) - Global level override file            (<install dir>/conf/etl_overrides.ksh)                    |
#                    10. (J) - Global Jobname override file          (<install dir>/conf/etl_overrides.jobname.<jobname>.ksh)  |
#                    11. (P) - Global Project override file          (<install dir>/conf/etl_overrides.project.<project>.ksh)  |
#                    12. (B) - Global Branch override file           (<install dir>/conf/etl_overrides.branch.<branch>.ksh)   _|_
#                    13. (S) - Global System level override file     (<install dir>/conf/etl_overrides.system.<system>.ksh)   \ /
#                    14. (*) - Default values                                                                                  V
#
# Parameters  : Type etl_wrapper.ksh --help
#
# To Do
# --------------
# Lock management     - release lock on interrupts etc
# Lock management     - configurable user or environment specific locks, sleep times, attempts etc
# Resource management - release resource on interrupts etc
# Resource management - configurable user or environment specific locks, sleep times, attempts etc
# Resource management - validation, check pool can have x number of units allocated
# Validation          - Externalize all validation functions so users can modify them easily
#
#
# Change History
# --------------
#
# Date         Author             Version    Description
# ------------ ------------------ ---------- ------------------------------------------------------------------------
# 04/02/2012   Philip Bowditch    1.0        Initial Version
# 20/01/2014   Philip Bowditch    1.1        Cleanup and added additional override file options
# 18/02/2015   Philip Bowditch    1.2        Fixed minor bugs
#--------------------------------------------------------------------------------------------------------------------

#--------------------------------------------------------------------------------------------------------------------
#  Set up global error codes
#--------------------------------------------------------------------------------------------------------------------

#--------------------------------------------------------------------------------------------------------------------
#  Set up common functions
#--------------------------------------------------------------------------------------------------------------------

function lock_enter
{
    #-----------------------------------------------------------------------------
    # Details    : To ensure this script is configured correctly set key variable
    #              values to defaults if they have not already been set somewhere
    #
    # Parameters : -sleep <seconds> - The number of seconds to sleep between requests
    #              -attempts <int>  - The number of attempts to try before failing
    #              -name <string>   - The name of the lock
    #              -user <string>   - If specified the user specific name
    #-----------------------------------------------------------------------------

    SLEEP_AMT=8
    RETRIES_AMT=1
    USERNAME=""
    LOCKNAME=""
    REASON=""
    LOCK_DIR=$(dirname "${PROGRAM_DIR}")/var/locks
    LOCK_FILE="${LOCK_DIR}/${LOCKNAME}.${USERNAME}.lock"

    # Go through all options
    while [[ $# -gt 0 ]]
    do

        case "$1" in

            --sleep     ) SLEEP_AMT="$2"
                          ;;
            --attempts  ) RETRIES_AMT="$2"
                          ;;
            --name      ) LOCKNAME="$2"
                          ;;
            --user      ) USERNAME="$2"
                          ;;
            --reason    ) REASON="$2"
                          ;;
            --lockpath  ) LOCK_FILE="$2"
                          LOCK_DIR=$(dirname "${LOCK_FILE}")
                          ;;

        esac

        shift 2
    done

    # Ensure lock directory exists, possibly not needed if a correct install can be guaranteed
    mkdir -p "${LOCK_DIR}/"

    # Attempt to perform a global lock, try once or possibly N times until success
    lockfile -${SLEEP_AMT:-8} -r "${RETRIES_AMT:-1}" "${LOCK_FILE:-$LOCK_DIR/$LOCKNAME.$USERNAME.lock}" 2>/dev/null
    if [[ $? -ne 0 ]]
    then
        log_error "INITIALISING" "Lock ${LOCK_FILE} could not be obtained"
        exit ${EXIT_CODE_LOCK_RETRY_FAILURE}
    fi

    # Create a log of every lock, in the format DATETIME=<dt>|PID=<pid>|ETL_ID=<id>|USER=<user>
    printf "DATETIME=%s|PID=%s|ETL_ID=%s|USER=%s|ACTION=%s|REASON=%s\n" $(date +%Y-%m-%d_%H:%M:%S) $$ "${ETL_ID}" "${CURRENT_USER}" GRANTED "${REASON}" >> "${LOCK_FILE}.log"

    return 0
}

function lock_exit
{
    #-----------------------------------------------------------------------------
    # Details    :
    #
    # Parameters :
    #-----------------------------------------------------------------------------

    USERNAME=""
    LOCKNAME=""
    REASON=""
    LOCK_DIR=$(dirname "${PROGRAM_DIR}")/var/locks
    LOCK_FILE="${LOCK_DIR}/${LOCKNAME}.${USERNAME}.lock"

    # Go through all options
    while [[ $# -gt 0 ]]
    do

        case "$1" in

            --name      ) LOCKNAME="$2"
                          ;;
            --user      ) USERNAME="$2"
                          ;;
            --reason    ) REASON="$2"
                          ;;
            --lockpath  ) LOCK_FILE="$2"
                          LOCK_DIR=$(dirname "${LOCK_FILE}")
                          ;;

        esac

        shift 2
    done

    LOCK_FILE="${LOCK_FILE:-$LOCK_DIR/$LOCKNAME.$USERNAME.lock}"

    printf "DATETIME=%s|PID=%s|ETL_ID=%s|USER=%s|ACTION=%s|REASON=%s\n" $(date +%Y-%m-%d_%H:%M:%S) $$ "${ETL_ID}" "${CURRENT_USER}" RELEASED "${REASON}" >> "${LOCK_FILE}.log"

    # Remove the lock
    rm -f "${LOCK_FILE}"
    if [[ $? -ne 0 ]]
    then
        log_error "INITIALISING" "Lock ${LOCK_FILE} could not be released"
        exit ${EXIT_CODE_LOCK_RETRY_FAILURE}
    fi
}

function resource_request
{
    #-----------------------------------------------------------------------------
    # Details    :
    #
    # Parameters :
    #-----------------------------------------------------------------------------

    SLEEP_AMT=8
    RETRIES_AMT=1
    USERNAME=""
    set -A RESOURCES
    RESOURCE_DIR=$(dirname "${PROGRAM_DIR}")/var/resource/
    RESOURCE_DELIMITER="="

    # Go through all options
    while [[ $# -gt 0 ]]
    do

        case "$1" in

            --sleep    ) SLEEP_AMT="$2";   shift 2
                         ;;
            --attempts ) RETRIES_AMT="$2"; shift 2
                         ;;
            --user     ) USERNAME="$2";    shift 2
                         ;;
            *          ) set -A RESOURCES $(printf "%s\n" "${@}" | sort)
                         shift $#
                         ;;

        esac

    done

    # Ensure resource pool directory exists
    mkdir -p "${RESOURCE_DIR}/"

    ii=0
    loops=0
    set -A RELEASE_LIST

    # Go through all resources requested
    while [[ "${ii}" -lt "${#RESOURCES[@]}" ]]
    do

        # May eventually use this to determine retries
        loops=$(( loops + 1 ))

        # Get the name of the resource requested and how many units are required
        RESOURCE=$( printf "%s\n" "${RESOURCES[$ii]}" | cut -d${RESOURCE_DELIMITER} -f1 )
        UNITS=$(    printf "%s\n" "${RESOURCES[$ii]}" | cut -d${RESOURCE_DELIMITER} -f2 )

        RESOURCE_FILE="${RESOURCE_DIR}/${RESOURCE}.${USERNAME}"

        NUM_ALLOCATED_ALL=0               # Holds the total number of units allocated from this resource
        NUM_ALLOCATED_PROCESS=0           # Holds the total number of units allocated to this ETL_ID already (incase multiple requests are made)
        POOL_LIMIT=0                      # How much is in the pool
        ALLOCATED_VALUE=0                 # How much do we want to grab (considering how many we may already have)

        lock_enter --sleep "${SLEEP_AMT:-8}" --attempts "${RETRIES_AMT:-1}" --user "${USERNAME}" --name "${RESOURCE}" --reason "${RESOURCE}"

            if [[ $? -eq 0 ]]
            then

                # Find out how many units within this pool and how many allocated so far, if files dont exist then touch them
                POOL_LIMIT=$(            awk -F"${RESOURCE_DELIMITER}" '{print $2}'                                                  "${RESOURCE_FILE}.pool"      2>/dev/null || printf "LIMIT=%s\n" 1 > "${RESOURCE_FILE}.pool" )
                NUM_ALLOCATED_ALL=$(     awk -F"${RESOURCE_DELIMITER}" 'BEGIN{cnt=0}{cnt+=$7}END{print cnt}'                         "${RESOURCE_FILE}.allocated" 2>/dev/null || touch "${RESOURCE_FILE}.allocated" )
                NUM_ALLOCATED_PROCESS=$( awk -F"${RESOURCE_DELIMITER}" "BEGIN{cnt=0} /^\|ETL_ID=$ETL_ID\|/ {cnt+=\$7}END{print cnt}" "${RESOURCE_FILE}.allocated" 2>/dev/null )

                ALLOCATED_VALUE=$(( UNITS + NUM_ALLOCATED_ALL ))

            else

                # We didn't get this resource so have to decrement the current count
                ii=$(( ii - 1 ))

                # Used to simply indicate a failure and we need to release all resources held
                ALLOCATED_VALUE=$(( POOL_LIMIT + 1 ))

            fi

            # If we couldn't get the resource or we are over our limit then start again by releasing everything
            if [[ "${ALLOCATED_VALUE:-0}" -gt "${POOL_LIMIT:-1}" ]]
            then

                # Only bother releasing if we actually have any
                if [[ "${#RELEASE_LIST[@]}" -gt 0 ]]
                then

                    # Release all held resources, this ensures we minimise deadlock incase other processes
                    # Are waiting on fewer resources
                    resource_release --sleep "${SLEEP_AMT}" --attempts "${RETRIES_AMT}" --user "${USERNAME}" "${RELEASE_LIST[@]}"

                    # Reset our variable that holds resources already allocated
                    unset RELEASE_LIST

                    ii=0
                fi

                lock_exit --user "${USERNAME}" --name "${RESOURCE}" --reason "${RESOURCE}"

                # Start again from the first resource
                continue
            fi

            # Update our amount if we already have some units allocated
            if [[ "${NUM_ALLOCATED_PROCESS}" -gt 0 ]]
            then

                # Remove the existing allocation line in the file so we are ready to append the updated version
                grep -v "\|ETL_ID=${ETL_ID}\|" "${RESOURCE_FILE}.allocated" > "${RESOURCE_FILE}.allocated.new"
                mv "${RESOURCE_FILE}.allocated.new" "${RESOURCE_FILE}.allocated"

                NUM_ALLOCATED_PROCESS=$(( NUM_ALLOCATED_PROCESS + UNITS ))

            fi

            # Allocate a resource by adding the allocation to the file
            printf "DATETIME=%s|PID=%s|ETL_ID=%s|USER=%s|ACTION=%s|UNIT=%s\n" $(date +%Y-%m-%d_%H:%M:%S) $$ "${ETL_ID}" "${CURRENT_USER}" GRANTED "${NUM_ALLOCATED_PROCESS}" | tee -a "${RESOURCE_FILE}.log" >> "${RESOURCE_FILE}.allocated"

        lock_exit --user "${USERNAME}" --name "${RESOURCE}" --reason "${RESOURCE}"

        RELEASE_LIST[${#RELEASE_LIST[@]}]="${RESOURCES[$ii]}"

        ii=$(( ii + 1 ))

    done
}

function resource_release
{
    #-----------------------------------------------------------------------------
    # Details    :
    #
    # Parameters :
    #-----------------------------------------------------------------------------

    SLEEP_AMT=8
    RETRIES_AMT=1
    USERNAME=""
    set -A RESOURCES
    RESOURCE_DIR=$(dirname "${PROGRAM_DIR}")/var/resource/

    # Go through all the options
    while [[ $# -gt 0 ]]
    do

        case "$1" in

            --sleep    ) SLEEP_AMT="$2";   shift 2
                         ;;
            --attempts ) RETRIES_AMT="$2"; shift 2
                         ;;
            --user     ) USERNAME="$2";    shift 2
                         ;;
            *          ) set -A RESOURCES $(printf "%s\n" "${@}" | sort -r)
                         shift $#
                         ;;

        esac

    done

    # Ensure resource pool directory exists
    mkdir -p "${RESOURCE_DIR}/"

    ii=0

    while [[ "${ii}" -lt "${#RESOURCES[*]}" ]]
    do

        RESOURCE=$( printf "%s\n" "${RESOURCES[$ii]}" | cut -d= -f1 )
        UNITS=$(    printf "%s\n" "${RESOURCES[$ii]}" | cut -d= -f2 )

        RESOURCE_FILE="${RESOURCE_DIR}/${RESOURCE}.${USERNAME}"

        lock_enter --sleep "${SLEEP_AMT:-8}" --attempts "${RETRIES_AMT:-1}" --user "${USERNAME}" --name "${RESOURCE}" --reason "${RESOURCE}"

            grep -v "\|ETL_ID=${ETL_ID}\|" "${RESOURCE_FILE}.allocated" > "${RESOURCE_FILE}.allocated.new"
            mv "${RESOURCE_FILE}.allocated.new" "${RESOURCE_FILE}.allocated"

            printf "DATETIME=%s|PID=%s|ETL_ID=%s|USER=%s|ACTION=%s|UNIT=%s\n" $(date +%Y-%m-%d_%H:%M:%S) $$ "${ETL_ID}" "${CURRENT_USER}" RELEASED "${UNITS}" >> "${RESOURCE_FILE}.log"

        lock_exit --user "${USERNAME}" --name "${RESOURCE}" --reason "${RESOURCE}"

        ii=$(( ii + 1 ))

    done
}

function check_relative_path
{
    #-----------------------------------------------------------------------------
    # Details    : Given an existing relative path, this function trys to ensure
    #              the result is always a full path.
    #
    # Parameters : 1 - The path to check
    # Returns    : stdout    - The full path if found
    #-----------------------------------------------------------------------------

    FILEPATH="${1}"

    # Replace home directory
    # . directories
    FILEPATH=$(printf "%s" "${FILEPATH}" | sed "s|^~/|${HOME}/|g")
    FILEPATH=$(printf "%s" "${FILEPATH}" | sed "s|/\./|/|g")
    FILEPATH=$(printf "%s" "${FILEPATH}" | sed "s|/+|/|g")

    FILEDIR=$(dirname "${FILEPATH}")
    FILEBASE=$(basename "${FILEPATH}")

    # If we can see the path exists we use pwd to determine the absolute path
    if [[ -e "${FILEDIR}" ]]
    then
        FILEDIR=$( cd "${FILEDIR}" && pwd -P )

        if [[ -n "${FILEDIR}" ]]
        then
            echo "${FILEDIR}/${FILEBASE}"
            return
        fi

    fi

    # Path did not exist or could not be understood, try whence as a backup method
    FILEPATH=$(whence "${1}")

    # Whence does not appear to be standard on all korn shell systems, it may produce no output
    # if so, not much we can do so return the path unchanged
    if [[ -z "${FILEPATH}" ]]
    then
        FILEPATH="${1}"
    fi

    echo "${FILEPATH}"
}

function ends_with
{
    #-----------------------------------------------------------------------------
    # Details    : This function returns whether the first parameter passed ends
    #              with the value in the second parameter
    #
    # Parameters : 1 - The string to check
    #              2 - the string to search for
    # Returns    : errorcode - 0 if starts with else 1
    #-----------------------------------------------------------------------------

    TEMP_RES=$(printf "%s" "${1}" | sed "s|${2}\$||g")
    if [[ "${TEMP_RES}" != "${1}" ]]
    then
        return 0
    else
        return 1
    fi
}

function starts_with
{
    #-----------------------------------------------------------------------------
    # Details    : This function returns whether the first parameter passed starts
    #              with the value in the second parameter
    #
    # Parameters : 1 - The string to check
    #              2 - the string to search for
    # Returns    : errorcode - 0 if starts with else 1
    #-----------------------------------------------------------------------------

    TEMP_RES=$(printf "%s" "${1}" | sed "s|^${2}||g")
    if [[ "${TEMP_RES}" != "${1}" ]]
    then
        return 0
    else
        return 1
    fi
}

function log_message
{
    #-----------------------------------------------------------------------------
    # Details    : Given one or more messages this function outputs them in a
    #              standard format, useful for automation and scripting
    #
    # Parameters : 1  - The type of message this is e.g. ERROR, INFO etc
    #              2  - The subtype or stage of the message e.g. VALIDATION,
    #                   INITIALISATION etc
    #              3+ - The messages to display
    # Returns    : stdout - The formatted message string
    #-----------------------------------------------------------------------------

    MSG_TYPE="${1}"
    MSG_SUB_TYPE="${2}"

    shift 2

    printf "-------------------------------------------------------------------------------------------------"

    while [[ $# -gt 0 ]]
    do

        printf "%s: %s: %s: %s\n" "${MSG_TYPE}" $(date +%Y%m%d%H%M%S) "${MSG_SUB_TYPE}" "$1"
        shift

    done

    printf "-------------------------------------------------------------------------------------------------"
}

function log_error
{
    #-----------------------------------------------------------------------------
    # Details    : Given one or more error messages this function outputs them in
    #              a standard format, useful for automation and scripting
    #
    # Parameters : 1  - The subtype or stage of the message e.g. VALIDATION,
    #                   INITIALISATION etc
    #              2+ - The messages to display
    # Returns    : stdout - The formatted message string
    #-----------------------------------------------------------------------------

    MSG_SUB_TYPE="${1}"

    shift

    log_message "ERROR" "${MSG_SUB_TYPE}" "$@" >&2
}

function log_info
{
    #-----------------------------------------------------------------------------
    # Details    : Given one or more info messages this function outputs them in
    #              a standard format, useful for automation and scripting
    #
    # Parameters : 1  - The subtype or stage of the message e.g. VALIDATION,
    #                   INITIALISATION etc
    #              2+ - The messages to display
    # Returns    : stdout - The formatted message string
    #-----------------------------------------------------------------------------

    MSG_SUB_TYPE="${1}"

    shift

    log_message "INFO" "${MSG_SUB_TYPE}" "$@"
}

function log_warning
{
    #-----------------------------------------------------------------------------
    # Details    : Given one or more warning messages this function outputs them in
    #              a standard format, useful for automation and scripting
    #
    # Parameters : 1  - The subtype or stage of the message e.g. VALIDATION,
    #                   INITIALISATION etc
    #              2+ - The messages to display
    # Returns    : stdout - The formatted message string
    #-----------------------------------------------------------------------------

    MSG_SUB_TYPE="${1}"

    shift

    log_message "WARNING" "${MSG_SUB_TYPE}" "$@"
}

function contains_directory
{
    #-----------------------------------------------------------------------------
    # Details    : Given a file path and a filename, this function will check the
    #              path to see if it contains a folder of the relevent name
    #
    # Parameters : 1 - The path to check
    #              2 - The name to find
    # Returns    : 0 - Name found
    #              1 - Not found
    #-----------------------------------------------------------------------------

    # Strip trailing directory delimiters
    SEARCH_DIR=$(printf "%s" "${1}" | sed 's|/$||g')
    SEARCH_FOR=$(printf "%s" "${2}" | sed 's|/$||g')

    # Check if the directory is within the path or at the end of the path etc
    starts_with "${SEARCH_DIR}" "/${SEARCH_FOR}/" || starts_with "${SEARCH_DIR}" "/${SEARCH_FOR}\$" || starts_with "${SEARCH_DIR}" "^${SEARCH_FOR}\$"
}
