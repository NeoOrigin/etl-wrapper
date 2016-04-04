
#!/bin/sh
#--------------------------------------------------------------------------------------------------------------------
# Script      : abinitio.sh
# Summary     : A generic script to call an abinitio process
# Details     : This script contains all of the parameters to run an abinitio graph, pset, plan, ksh etc
# Parameters  : Type abinitio.sh --help
#
# Change History
#
# Date         Author             Version    Description
# ------------ ------------------ ---------- ------------------------------------------------------------------------
# 22/10/2010   Philip Bowditch    1.0        Initial Version
# 16/11/2010   Philip Bowditch    1.1        Updated to handle plan psets
# 10/02/2011   Philip Bowditch    1.2        Added branch following eme re-org
# 24/08/2011   Philip Bowditch    1.3        Added additional handling for non standard sandbox paths
# 11/10/2011   Philip Bowditch    1.4        Deduping log file dumps incase a plan outputs graph stdout messages
#--------------------------------------------------------------------------------------------------------------------

#--------------------------------------------------------------------------------------------------------------------
#  Set up global error codes
#--------------------------------------------------------------------------------------------------------------------

# Create abinitio specific error codes
EXIT_CODE_NON_EXECUTABLE_FILE=20                 # The program/script to run is not executable
EXIT_CODE_AIR_CMD_FAILED=21                      # Helper sandbox air commands failed
EXIT_CODE_NO_SANDBOX_FOUND=22                    # No sandbox could be determined
EXIT_CODE_SANDBOX_INIT_FAILED=23                 # sandbox could not be initialised

# Mimic the etl_wrappers more common error codes
EXIT_CODE_FILE_DOES_NOT_EXIST=50                 # The program to run does not exist
EXIT_CODE_NOT_A_FILE=51                          # The program to run not a file or symbolic link
EXIT_CODE_UNREADABLE_FILE=52                     # The program to run is not readable
EXIT_CODE_PROGRAM_NOT_SUPPORTED=53               # The program is not supported by this script

# Mimic the etl wrappers validation error codes
EXIT_CODE_INVALID_ETL_BASE_DIR=101               # ETL_BASE_DIR parameter is invalid
EXIT_CODE_INVALID_ETL_BRANCH=102                 # ETL_BRANCH parameter is invalid
EXIT_CODE_INVALID_ETL_PROGRAM=107                # ETL_PROGRAM parameter is invalid
EXIT_CODE_INVALID_ETL_PROJECT=108                # ETL_PROJECT parameter is invalid
EXIT_CODE_INVALID_ETL_REL_PATH=109               # ETL_REL_PATH parameter is invalid
EXIT_CODE_INVALID_ETL_PRIORITY=115               # ETL_PRIORITY parameter is invalid


#--------------------------------------------------------------------------------------------------------------------
#  Set up common functions
#--------------------------------------------------------------------------------------------------------------------

function usage
{
    #-----------------------------------------------------------------------------
    # Details    : This function outputs the usage information detailing how to
    #              call this script.
    #
    # Parameters : None
    #-----------------------------------------------------------------------------

    cat << EOF
   ${PROGRAM} 
      ( -h | -help )                                           - Displays this usage statement and exits

   Examples

     1. The following runs the myplan1 plan in the pos_outbound project which is located
        under the path \$HOME/sand/test/JLP/JL/POS/pos_outbound/plan/myplan1.plan

          ${PROGRAM} \$HOME/sand test JLP/JL/POS pos_outbound plan/myplan1.plan

     2. The following runs the extract_mainframe hotfix graph under the same project

          ${PROGRAM} \$HOME/sand test JLP/JL/POS pos_outbound mp/fixes/extract_mainframe.mp

     3. The following runs the above graph, instanced by a particular pset

          ${PROGRAM} \$HOME/sand test JLP/JL/POS pos_outbound pset/extract_mainframe.pset

     4. The following runs a custom korn shell script within the \$AI_BIN directory

          ${PROGRAM} \$HOME/sand test JLP/JL/POS pos_outbound bin/run_program.ksh

     5. The following turns on tracking and ignores validation errors for the ab initio graph
        the underlying DATE_ID parameter for the graph is also given a value on the command line

          ${PROGRAM} -v IGNORE -t true \$HOME/sand test JLP/JL/POS pos_outbound mp/extract_mainframe.mp -DATE_ID 20101027

     6. The following performs validation on the ab initio pset, but does not run the pset

          ${PROGRAM} -v WARN -x false \$HOME/sand test JLP/JL/POS pos_outbound mp/extract_mainframe.mp -DATE_ID 20101027
EOF
}

function validate_program
{
    #-----------------------------------------------------------------------------
    # Details    : 
    #
    # Parameters : None
    #-----------------------------------------------------------------------------

    ETL_PROGRAM_FULL_PATH=$1

    if [[ "${ETL_VALIDATION_LEVEL}" != "IGNORE" ]]
    then

        #
        # Check if the program exists, if its a file or symbolic link and if its readable
        #
        if [[ ! -e ${ETL_PROGRAM_FULL_PATH} ]]
        then
   
            MSG="File '${ETL_PROGRAM_FULL_PATH}' does not exist"

            if [[ "${ETL_VALIDATION_LEVEL}" == "WARN" ]]
            then
                log_warning "VALIDATE" "${MSG}"
            else
                log_error   "VALIDATE" "${MSG}" ${EXIT_CODE_FILE_DOES_NOT_EXIST}
            fi

        fi

        if [[ ! -f ${ETL_PROGRAM_FULL_PATH} && ! -L ${ETL_PROGRAM_FULL_PATH} ]]
        then
   
            MSG="File '${ETL_PROGRAM_FULL_PATH}' is not a regular file"

            if [[ "${ETL_VALIDATION_LEVEL}" == "WARN" ]]
            then
                log_warning "VALIDATE" "${MSG}"
            else
                log_error   "VALIDATE" "${MSG}" ${EXIT_CODE_NOT_A_FILE}
            fi
   
        fi

        if [[ ! -r ${ETL_PROGRAM_FULL_PATH} ]]
            then

            MSG="File '${ETL_PROGRAM_FULL_PATH}' is not readable"

            if [[ "${ETL_VALIDATION_LEVEL}" == "WARN" ]]
            then
                log_warning "VALIDATE" "${MSG}"
            else
                log_error   "VALIDATE" "${MSG}" ${EXIT_CODE_UNREADABLE_FILE}
            fi
   
        fi

    fi
}

function validate_ai_program
{
    #-----------------------------------------------------------------------------
    # Details    : 
    #
    # Parameters : None
    #-----------------------------------------------------------------------------

    ETL_PROGRAM_FULL_PATH=$1

    if [[ "${ETL_VALIDATION_LEVEL}" != "IGNORE" ]]
    then

        MSG="Validation of '${ETL_PROGRAM_FULL_PATH}' failed"

        if [[ "${ETL_VALIDATION_LEVEL}" == "WARN" ]]
        then
            trap 'log_warning "VALIDATE" "${MSG}"' HUP INT QUIT TERM ERR
        else
            trap 'log_error "VALIDATE" "${MSG}" $?' HUP INT QUIT TERM ERR
        fi

        # If target is a pset then determine what to do by what it points at
        if [[ "${ETL_PROGRAM_EXT}" == "pset" ]]
        then
            PSET_PROTOTYPE=$(head -1 "${ETL_PROGRAM_FULL_PATH}")
            ETL_PROGRAM_EXT=${PSET_PROTOTYPE:##*\.}
        fi

        # Check file type and perform validation as a plan or as a graph
        if [[ "${ETL_PROGRAM_EXT}" == "plan" ]]
        then
            air sandbox run ${ETL_PROGRAM_FULL_PATH} -validate-only "${@}"
        else
            air sandbox validate-graph ${ETL_PROGRAM_FULL_PATH} -strict
        fi

        # Reset the trap incase it was changed to handle warnings
        trap - HUP INT QUIT TERM ERR
        MSG=""

    fi
}

function validate_native_program
{
    #-----------------------------------------------------------------------------
    # Details    : 
    #
    # Parameters : None
    #-----------------------------------------------------------------------------

    ETL_PROGRAM_FULL_PATH=$1

    if [[ "${ETL_VALIDATION_LEVEL}" != "IGNORE" ]]
    then

        # Is this non ab initio file executable
        if [[ ! -x ${ETL_PROGRAM_FULL_PATH} ]]
        then

            MSG="File '${ETL_PROGRAM_FULL_PATH}' is not executable"

            if [[ "${ETL_VALIDATION_LEVEL}" == "WARN" ]]
            then
                log_warning "VALIDATE" "${MSG}"
            else
                log_error   "VALIDATE" "${MSG}" ${EXIT_CODE_NON_EXECUTABLE_FILE}
            fi

            MSG=""

        fi

        # If a script check its syntax
        if [[ "${ETL_PROGRAM_EXT}" == "ksh" ]]
        then

            MSG="Validation of '${ETL_PROGRAM_FULL_PATH}' failed"

            if [[ "${ETL_VALIDATION_LEVEL}" == "WARN" ]]
            then
                trap 'log_warning "VALIDATE" "${MSG}"' HUP INT QUIT TERM ERR
            else
                trap 'log_error "VALIDATE" "${MSG}" $?' HUP INT QUIT TERM ERR
            fi

            ${SHELL} -n ${ETL_PROGRAM_FULL_PATH}

            trap - HUP INT QUIT TERM ERR
            MSG=""

        fi

    fi
}

function find_process_parent
{
    # Find out where we were called from by looking at all processes that led to this script being called
    # if we find a ksh file being executed we save its path so it can help find our sandbox if necessary
    TEMP_SEARCH_PATH=""
    TEMP_PID=$$
    TEMP_CMD=""
    while [[ ${TEMP_PID:-0} -gt 0 ]]
    do
        TEMP_OUT=$(ps -o "%P,%a" -p ${TEMP_PID} | tail -1)
        if [[ $? -ne 0 || -z "${TEMP_OUT}" ]]
        then
            break
        fi

        TEMP_PID=$(echo "${TEMP_OUT}" | cut -d, -f1)
        TEMP_CMD=$(echo "${TEMP_OUT}" | cut -d, -f2-)

        starts_with "${TEMP_CMD}" "${SHELL} "
        if [[ $? -eq 0 ]]; then
            TEMP_SCRIPT=$(printf "%s" "${TEMP_CMD}" | cut -d" " -f2)
            TEMP_SCRIPT=$(dirname "${TEMP_SCRIPT}")

            TEMP_SEARCH_PATH="${TEMP_SCRIPT} ${TEMP_SEARCH_PATH}"
        fi
    done

    echo "${TEMP_SEARCH_PATH}"
}

function find_sandbox
{
    #-----------------------------------------------------------------------------
    # Details    : Given a list of paths this function attempts to find any sandbox
    #              that they sit within
    #
    # Parameters : * - The paths to check
    # Returns    : stderr - Any error messages that prevent the function completing
    #              stdout - The path to the sandbox if found
    #-----------------------------------------------------------------------------

    TEMP_PROJ=""

    # Try to find a sandbox from all the paths passed in
    while [[ "${#}" -gt 0 ]]
    do
        TEMP_PATH="$1"
        
        if [[ ! -e "${TEMP_PATH}" ]]
        then
            log_error "SEARCHING" "Could not find target program, ensure ETL_PROGRAM points to a full path, cd to a sandbox, pass a configuration file in the same sandbox or set ETL_PROJECT and ETL_REL_PATH explicitly" ${EXIT_CODE_FILE_DOES_NOT_EXIST}
        fi

        # All we have to go on is a file, try to find the nearest sandbox it sits in
        TEMP_PROJ=$(air sandbox find "${TEMP_PATH}" -up)
        if [[ $? -ne 0 ]]
        then
            log_error "SEARCHING" "Could not run air sandbox find command, please check your ab initio installation" ${EXIT_CODE_AIR_CMD_FAILED}
        fi

        # exit once sandbox found
        if [[ -n "${TEMP_PROJ}" ]]; then
            break
        fi

        shift
    done

    # Error if no parameters were passed as we have no sandbox files to query
    if [[ -z "${TEMP_PROJ}" ]]
    then
        log_error "SEARCHING" "Could not find a default sandbox, please set parameters appropriately" ${EXIT_CODE_NO_SANDBOX_FOUND}
    fi

    printf "%s\n" "${TEMP_PROJ}"
}

#trap 'ERR_CODE=$?; log_error "FAILURE" "${MSG}" ${ERR_CODE}' HUP INT QUIT TERM ERR

#--------------------------------------------------------------------------------------------------------------------
#  Initialise Global Variables
#--------------------------------------------------------------------------------------------------------------------

# unset / blank out temporary variables

unset MSG \
      ETL_PARAMS

#--------------------------------------------------------------------------------------------------------------------
#  Main
#
#  1 - Parse Command Line for options and setup variables
#  2 - If information ot complete then try to infer information from what IS available
#  3 - Perform validation (if necessary) generic to all file based processes
#  4 - Determine type of process and perform validation (if necessary) for that process
#  5 - Run ETL program
#--------------------------------------------------------------------------------------------------------------------

# Check if any options were passed otherwise exit and proceed on first unknown
while [[ "${#}" -gt 0 ]]
do

   case "$1" in

      -h | --help                 ) usage
                                     exit 0
                                     ;;
      *                            ) break
                                     ;;

   esac

done


ETL_PROGRAM_DIR=${ETL_PROGRAM:%/*}         # The relative directory of the program to run from the project base, no filename
ETL_PROGRAM_BASENAME=${ETL_PROGRAM:##*/}   # The basename of the program to run, no relative directory
ETL_PROGRAM_EXT=${ETL_PROGRAM:##*\.}       # The file extension of the program to run



# ETL_PROJECT, ETL_REL_PATH and ETL_BASE_DIR are essential for determining what
# to run. If they aren't set we try to determine these directly from the two paths
# availble to us, firstly the program, if its a relative path itself we try the include
# file
if [[ -z "${ETL_PROJECT}" || -z "${ETL_REL_PATH}" || -z "${ETL_BASE_DIR}" ]]
then

    # Try to get the full file path to the underlying program with whatever variables are
    # defined as it may have been passed as a relative path.
    TEMP_PATH=""
    if [[ -n "${ETL_BASE_DIR}"         ]]; then TEMP_PATH="${ETL_BASE_DIR}/";                    fi
    if [[ -n "${ETL_BRANCH}"           ]]; then TEMP_PATH="${TEMP_PATH}${ETL_BRANCH}/";          fi
    if [[ -n "${ETL_REL_PATH}"         ]]; then TEMP_PATH="${TEMP_PATH}${ETL_REL_PATH}/";        fi
    if [[ -n "${ETL_PROJECT}"          ]]; then TEMP_PATH="${TEMP_PATH}${ETL_PROJECT}/";         fi
    if [[ -n "${ETL_PROGRAM_DIR}"      ]]; then TEMP_PATH="${TEMP_PATH}${ETL_PROGRAM_DIR}/";     fi
    if [[ -n "${ETL_PROGRAM_BASENAME}" ]]; then TEMP_PATH="${TEMP_PATH}${ETL_PROGRAM_BASENAME}"; fi
    ETL_PROGRAM_FULL_PATH=$(check_relative_path "${TEMP_PATH}")


    # Find out where we were called from by looking at all processes that led to this script being called
    # if we find a ksh file being executed we save its path so it can help find our sandbox if necessary
    TEMP_SEARCH_PATH=$(find_process_parent)

    # To get the missing directories we need to find a sandbox, first try the home of the executable
    # (dont worry if its not found), then the current directory and lastly any include files

    TEMP_PROJ=$(find_sandbox ${ETL_PROGRAM_FULL_PATH} ${PWD} ${PROJECT_DIR} ${TEMP_SEARCH_PATH})
    RC=$?
    if [[ $RC -ne 0 ]]
    then
        exit $RC
    else
        log_info "INFERRING" "Core variable/s are not defined"
        printf "Sandbox found at '%s'" "${TEMP_PROJ}"
    fi

    # By this point we should have a sandbox found, try to get its project name
    # Remove project name from the path as we have handled project names
    PROJ_BASENAME=$(basename "${TEMP_PROJ}")
    if [[ $? -ne 0 || -z "${ETL_PROJECT}" ]]
    then
        ETL_PROJECT=${ETL_PROJECT:-$PROJ_BASENAME}
        printf "%-20s (I) = %s\n" "ETL_PROJECT" "${ETL_PROJECT}"   
    elif [[ "${ETL_PROJECT}" != "${PROJ_BASENAME}" ]]
    then
        log_error "VALIDATE" "ETL_PROJECT '${ETL_PROJECT}' is not found in the sandbox path '${TEMP_PROJ}' is this incorrectly configured" ${EXIT_CODE_MISMATCHED_PROJECT_FOUND}
    fi
    TEMP_PROJ=$(dirname "${TEMP_PROJ}")


    # Whats left is the full path to the sandbox, this needs to be split into a base dir
    # a branch and a relative directory.  Branch is the most standardised/static, so check
    # for common known values e.g. main, test, release etc
    if [[ -n "${ETL_BRANCH}" ]]
    then
        # If we already have branch details then use them to see where in the structure branches could be
        TEMP_BRANCH_INDEX=$(echo "${TEMP_PROJ}" | awk "{print index(\$0, \"/${ETL_BRANCH}/\")}")

        if [[ "${TEMP_BRANCH_INDEX}" -le 0 ]]
        then
            log_error "VALIDATE" "ETL_BRANCH '${ETL_BRANCH}' is not found in the sandbox path '${TEMP_PROJ}' is this incorrectly configured" ${EXIT_CODE_MISMATCHED_BRANCH_FOUND}
        fi

        TEMP_BRANCH_INDEX=$(( TEMP_BRANCH_INDEX + 1 ))
    else
        # We dont know what branch it is so check for common standardised ones
        TEMP_BRANCH_MAIN_INDEX=$(echo "${TEMP_PROJ}" | awk '{print index($0, "/main/")}')
        TEMP_BRANCH_TEST_INDEX=$(echo "${TEMP_PROJ}" | awk '{print index($0, "/test/")}')
        TEMP_BRANCH_SIT_INDEX=$(echo  "${TEMP_PROJ}" | awk '{print index($0, "/sit/")}')
        TEMP_BRANCH_PSIT_INDEX=$(echo "${TEMP_PROJ}" | awk '{print index($0, "/pre_sit/")}')
        TEMP_BRANCH_LIVE_INDEX=$(echo "${TEMP_PROJ}" | awk '{print index($0, "/release/")}')
        TEMP_BRANCH_ABI_INDEX=$(echo  "${TEMP_PROJ}" | awk "{print index(\$0, \"/$AB_AIR_BRANCH/\")}")

        unset TEMP_BRANCH_INDEX

        # Find the smallest found flag (i.e. has to be greater than an index of 0)
        # compensate for 1 additional character in all the indexes i.e. the leading slash
          if [[ "${TEMP_BRANCH_ABI_INDEX}"  -gt 0 ]]; then TEMP_BRANCH_INDEX=$(( TEMP_BRANCH_ABI_INDEX  + 1 )); ETL_BRANCH="${AB_AIR_BRANCH}"
        elif [[ "${TEMP_BRANCH_LIVE_INDEX}" -gt 0 ]]; then TEMP_BRANCH_INDEX=$(( TEMP_BRANCH_LIVE_INDEX + 1 )); ETL_BRANCH="release"
        elif [[ "${TEMP_BRANCH_PSIT_INDEX}" -gt 0 ]]; then TEMP_BRANCH_INDEX=$(( TEMP_BRANCH_PSIT_INDEX + 1 )); ETL_BRANCH="pre_sit"
        elif [[ "${TEMP_BRANCH_SIT_INDEX}"  -gt 0 ]]; then TEMP_BRANCH_INDEX=$(( TEMP_BRANCH_SIT_INDEX  + 1 )); ETL_BRANCH="sit"
        elif [[ "${TEMP_BRANCH_TEST_INDEX}" -gt 0 ]]; then TEMP_BRANCH_INDEX=$(( TEMP_BRANCH_TEST_INDEX + 1 )); ETL_BRANCH="test"
        elif [[ "${TEMP_BRANCH_MAIN_INDEX}" -gt 0 ]]; then TEMP_BRANCH_INDEX=$(( TEMP_BRANCH_MAIN_INDEX + 1 )); ETL_BRANCH="main"
        fi

        if [[ -n "${TEMP_BRANCH_INDEX}" ]]
        then
            printf "%-20s (I) = %s\n" "ETL_BRANCH" "${ETL_BRANCH}"
        fi
    fi


    # Most sandboxes are under the user directory check if our project path
    # is under the $HOME/sand directory, if it is then $HOME/sand is the base
    # else accept the parent directory of the project as the base
    if [[ -z "${ETL_BASE_DIR}" ]]
    then
        if [[ -n "${ETL_BRANCH}" ]]
        then
            : ${ETL_BASE_DIR:=$(echo "${TEMP_PROJ}" | cut -c1-$(( TEMP_BRANCH_INDEX - 2 )))}
        else
            # Find out if the sandbox starts at a common location $HOME/sand
            starts_with "${TEMP_PROJ}" "${HOME}/sand"
            if [[ $? -eq 0 ]]
            then
                : ${ETL_BASE_DIR:=$HOME/sand}
            elif [[ -n "${ETL_REL_PATH}" ]]
            then
                : ${ETL_BASE_DIR:=$(echo "${TEMP_PROJ}" | sed "s|/${ETL_REL_PATH}$||g")}
            else
                # Bit of a hack here for john lewis, check the leading directories in their
                # enterprise path
                TEMP_JLP_INDEX=$(echo "${TEMP_PROJ}" | awk '{print index($0, "/JLP/")}')
                TEMP_JL_INDEX=$( echo "${TEMP_PROJ}" | awk '{print index($0, "/JL/")}')

                # Find the smallest found flag (i.e. has to be greater than an index of 0)
                # compensate for 1 additional character in all the indexes i.e. the leading slash
                  if [[ "${TEMP_JLP_INDEX}" -gt 0 ]]; then : ${ETL_BASE_DIR:=$(echo "${TEMP_PROJ}" | cut -c1-$(( TEMP_JLP_INDEX - 1 )))}
                elif [[ "${TEMP_JL_INDEX}"  -gt 0 ]]; then : ${ETL_BASE_DIR:=$(echo "${TEMP_PROJ}" | cut -c1-$(( TEMP_JL_INDEX  - 1 )))}
                else                                       : ${ETL_BASE_DIR:=$(dirname "${TEMP_PROJ}")}
                fi
            fi
        fi

        printf "%-20s (I) = %s\n" "ETL_BASE_DIR" "${ETL_BASE_DIR}"
    fi


    if [[ -z "${ETL_REL_PATH}" ]]
    then
        if [[ -n "${ETL_BRANCH}" ]]
        then
            BRANCH_SIZE=${#ETL_BRANCH}
            : ${ETL_REL_PATH:=$(echo "${TEMP_PROJ}" | cut -c$(( TEMP_BRANCH_INDEX + BRANCH_SIZE + 1 ))-)}
        else
            starts_with "${TEMP_PROJ}" "${ETL_BASE_DIR}"
            if [[ $? -eq 0 ]]
            then
                : ${ETL_REL_PATH:=$(echo "${TEMP_PROJ}" | sed "s|^${ETL_BASE_DIR}/||g")}
            else
                : ${ETL_REL_PATH:=$(basename "${TEMP_PROJ}")}
            fi
        fi

        printf "%-20s (I) = %s\n" "ETL_REL_PATH" "${ETL_REL_PATH}"
    fi

    export ETL_BASE_DIR
    export ETL_BRANCH
    export ETL_REL_PATH
    export ETL_PROJECT
fi



# Validate the incoming (possibly newly inferred) parameters
validate_etl_base_dir         "${ETL_BASE_DIR}"
validate_etl_branch           "${ETL_BRANCH}"
validate_etl_rel_path         "${ETL_REL_PATH}"
validate_etl_project          "${ETL_PROJECT}"
validate_etl_program          "${ETL_PROGRAM_BASENAME}"


# Normalise the path variables incase the user entered blank values
if [[ -n "${ETL_BASE_DIR}" ]];  then ETL_BASE_DIR="${ETL_BASE_DIR}/"; fi
if [[ -n "${ETL_BRANCH}"   ]];  then ETL_BRANCH="${ETL_BRANCH}/";     fi
if [[ -n "${ETL_REL_PATH}" ]];  then ETL_REL_PATH="${ETL_REL_PATH}/"; fi
if [[ -n "${ETL_PROJECT}"  ]];  then ETL_PROJECT="${ETL_PROJECT}/";   fi

# Only those non blank parameters will have directory characters so concatenating them together is the
# same as ignoring missing items
ETL_PROJECT_FULL_PATH="${ETL_BASE_DIR}${ETL_BRANCH}${ETL_REL_PATH}${ETL_PROJECT}"
ETL_DIRECTORY_FULL_PATH="${ETL_PROJECT_FULL_PATH}${ETL_PROGRAM_DIR}"
ETL_PROGRAM_FULL_PATH="${ETL_DIRECTORY_FULL_PATH}/${ETL_PROGRAM_BASENAME}"


#
# ETL_PRIORITY should be picked up by the environment variable, unlike NICE which controls local job
# priority e.g. shell scripts and non parallel code we need to set AB_NICE as this ensures priority level
# over heterogeneous / remote environments.
#
if [[ -n "${ETL_PRIORITY}" ]]
then
    log_info "VARIABLES" "Source: (R)un Script"
    export AB_NICE="${ETL_PRIORITY}"
    printf "%-20s (%s) = %-65s\n"  AB_NICE "R" "${AB_NICE}"
    validate_etl_priority "${ETL_PRIORITY}"
fi


# Determine whether to pass a flag to start tracking or not
unset AM_I_TRACKING
if [[ "${ETL_TRACKING}" == "true" ]]
then
    AM_I_TRACKING=" -reposit-tracking"
fi



ERROR_CODE=0
COPROCESS_ID=""

#
# If we are validating then check for simple file system errors e.g. file not found etc, these
# checks are generic to script AND ab initio executables so can be done upfront
#
validate_program "${ETL_PROGRAM_FULL_PATH}"


# Determine how to run the Program
case "${ETL_PROGRAM_EXT}" in

   mp | plan | pset ) validate_ai_program "${ETL_PROGRAM_FULL_PATH}" "${ETL_PARAMS[@]}"

                      if [[ "${ETL_EXECUTE}" == "true" ]]
                      then
                         # Run job in a coprocess / background process as we need to filter its output for the
                         # error log but also need to capture the return status which is difficult to do in a pipe
                         log_info "STARTING" "air sandbox run ${ETL_PROGRAM_FULL_PATH}${AM_I_TRACKING} ${ETL_PARAMS[*]}"
                         air sandbox run ${ETL_PROGRAM_FULL_PATH}${AM_I_TRACKING} "${ETL_PARAMS[@]}" 2>&1 |&
                         COPROCESS_ID=$!
                      else
                         log_warning "SKIPPING" "${ETL_PROGRAM_FULL_PATH}${AM_I_TRACKING} ${ETL_PARAMS[@]}"
                      fi
                      ;;

   sh | ksh         ) validate_native_program "${ETL_PROGRAM_FULL_PATH}"

                      # Try to determine where withing the sandbox the program sits, using this will help determine how to run it
                      ETL_DIRECTORY="${ETL_PROGRAM_FULL_PATH}"
                      ETL_DIRECTORY=$(dirname  "${ETL_DIRECTORY}")
                      ETL_DIRECTORY=$(basename "${ETL_DIRECTORY}")

                      # Perform a bit of a hack to recognize deployed graph shell scripts so that they also
                      # can reposit tracking, adding AM_I_TRACKING here should have no impact unless they sit
                      # inside a 'run' directory
                      if [[ "${ETL_DIRECTORY}" != "run" ]]
                      then
                          unset AM_I_TRACKING
                      fi

                      # As this is a script within an abinitio sandbox you can run the script simply by executing it
                      # however if it does not sit in the run directory then its not a graph so may need the sandbox
                      # initialised to refer to variables
                      if [[ "${ETL_EXECUTE}" == "true" ]]
                      then
                         # Initialise sandbox if its not a script in the run directory
                         if [[ "${ETL_DIRECTORY}" != "run" ]]
                         then
                             log_info "INITIALISING" ". ${ETL_PROJECT_FULL_PATH}/ab_project_setup.ksh {ETL_PROJECT_FULL_PATH}"
                             . "${ETL_PROJECT_FULL_PATH}/ab_project_setup.ksh" "{ETL_PROJECT_FULL_PATH}"
                             if [[ $? -ne 0 || -z "${PROJECT_DIR}" ]]
                             then
                                 log_error "INITIALISING" "${ETL_PROJECT} failed to initialise" ${EXIT_CODE_SANDBOX_INIT_FAILED}
                             fi
                         fi

                         log_info "STARTING" "${ETL_PROGRAM_FULL_PATH}${AM_I_TRACKING} ${ETL_PARAMS[@]}"
                         ${ETL_PROGRAM_FULL_PATH}${AM_I_TRACKING} "${ETL_PARAMS[@]}" 2>&1 |&
                         COPROCESS_ID=$!
                      else
                         log_warning "SKIPPING" "${ETL_PROGRAM_FULL_PATH}${AM_I_TRACKING} ${ETL_PARAMS[@]}"
                      fi
                      ;;

    *               ) log_error "VALIDATE" "Invalid file extension found ${ETL_PROGRAM_EXT}" ${EXIT_CODE_PROGRAM_NOT_SUPPORTED}
                      ;;
esac


# If a co process was run we need to wait for it to exit, additionally this can only happen if we
# run a command so hence it indicates also that a log file could have been created for us to query
if [[ -n "${COPROCESS_ID}" ]]
then
    ERROR_LOGS=""
    JOB_IDS_TO_CHECK=""

    # Loop through its output copying it to stdout, look for error logs just incase this job fails
    # and so we can output something meaningful. Because some of the error details useful in finding
    # an error log are sent on stderr by the plan there is no easy way to get both stdout and stderr
    # from a background/coprocess job.  So all output is redirected to stdout by the commands above
    while read -p outputline
    do
        print -r -- "$outputline"

        # Grep through the output to see if an error log can be found
        starts_with "${outputline}" "info : Error logging to "
        if [[ $? -eq 0 ]]; then
            NEW_LOG=$(echo "${outputline}" | awk '{print $6}')
            ERROR_LOGS="${ERROR_LOGS}\n${NEW_LOG}"
        else
            # Plans dont output individual error log details so extract the unique job ids
            # if any failures occur
            starts_with "${outputline}" "ERROR : ++++ FAILED ++++ Job "
            if [[ $? -eq 0 ]]; then
                TEMP_JOB_ID=$(echo "${outputline}" | awk '{print $7}')
                JOB_IDS_TO_CHECK="${JOB_IDS_TO_CHECK} ${TEMP_JOB_ID}"
            fi
        fi

    done

    # As it was a background process we wait for it to complete setting the global error code to
    # its result
    wait $COPROCESS_ID
    ERROR_CODE=$?

    # We wait until the job is finished before searching error logs, this ensures they have been
    # completely written to
    if [[ -n "${JOB_IDS_TO_CHECK}" ]]; then

        # The project might not be initialised directly, try to find the directory instead
        ERR_LOG_DIRECTORY="${AI_SERIAL_ERROR}"
        if [[ -z "${ERR_LOG_DIRECTORY}" ]]; then
            ERR_LOG_DIRECTORY=$(air sandbox parameter -basedir "${ETL_PROJECT_FULL_PATH}" -eval AI_SERIAL_ERROR 2>/dev/null)
        fi

        # Convert the seconds since this wrapper first started to number of minutes, offset by 1 minute
        # to ensure some overlap/buffer on the conversion, this helps limit the number of files that the
        # find command attempts to search within
        TMP_MIN=$(( ( SECONDS / 60 ) + 1 ))

        # find error logs updated in the last few minutes since this job ran
        # and check if that log has a reference to the unique job id, because jobs are restarted
        # only refer to the latest file
        for JOB_ID in ${JOB_IDS_TO_CHECK}; do

            NEW_LOG=$(find "${ERR_LOG_DIRECTORY}" -mmin +0 -mmin -${TMP_MIN} -type f -name "*.err" -exec grep -l "${JOB_ID}" {} \; 2>/dev/null | tail -1)
            if [[ $? -eq 0 && -n "${NEW_LOG}" ]]; then
                ERROR_LOGS="${ERROR_LOGS}\n${NEW_LOG}"
            fi

        done

    fi

    # All the jobs are finished, if any error log were found and if they have something
    # in them then output it, ensure we dedup the error logs as we dont want to output
    # error more than once
    for ERROR_LOG in $(echo "${ERROR_LOGS}" | sort -u); do

        if [[ -s "${ERROR_LOG}" ]]
        then
            log_error "ERROR MSG" "Dumping log file ${ERROR_LOG}"
            cat "${ERROR_LOG}" >&2
        fi

    done

fi


# Log the final status of the job, if it errored then also dump out the log file for analysis
# error code could still be zero even if nothing was run e.g. ETL_EXECUTE was false
if [[ $ERROR_CODE -eq 0 ]]
then
    log_info  "ENDING" "Passed, return code ${ERROR_CODE}, elapsed ${SECONDS} seconds"
else
    log_error "ENDING" "Failed, return code ${ERROR_CODE}, elapsed ${SECONDS} seconds"
fi


return ${ERROR_CODE}