#!/usr/bin/env bash

# A Bash script with many useful functions. This file is suitable for sourcing
# into other scripts and so only contains functions which are unlikely to need
# modification. It omits the following functions:
# - main()
# - parseScriptOptions()
# - printScriptUsage()


#+----------------------------------------------------------------------------------------------------------+
# Functions

# DESC: Generic script initialisation
# ARGS: $@ (optional): Arguments provided to the script
# OUTS: $orig_cwd: The current working directory when the script was run
#       $script_path: The full path to the script
#       $script_dir: The directory path of the script
#       $script_name: The file name of the script
#       $script_params: The original parameters provided to the script
#       $ta_none: The ANSI control code to reset all text attributes
# NOTE: $script_path only contains the path that was used to call the script
#       and will not resolve any symlinks which may be present in the path.
#       You can use a tool like realpath to obtain the "true" path. The same
#       caveat applies to both the $script_dir and $script_name variables.
# shellcheck disable=SC2034
# SOURCE: https://github.com/ralish/bash-script-template/blob/stable/source.sh
function initScript() {
    # App infos
    app_name="SINP Server Data"
    app_code="ssd"

    # Script time
    readonly time_start="$(date +%s)"
    readonly fmt_time_start="$(date -d @${time_start} "+%Y-%m-%d %H:%M:%S")"

    # Useful paths
    readonly orig_cwd="$PWD"
    readonly script_path=$(realpath "${BASH_SOURCE[1]}")
    readonly script_dir="$(cd "$(dirname "${script_path}")" && pwd -P)"
    readonly script_name="$(basename "$script_path")"
    readonly script_params="$*"

    #+----------------------------------------------------------------------------+
    # Directories pathes
    readonly current_lib_path="$(realpath "${BASH_SOURCE[0]}")"
    readonly lib_shared_dir=$(realpath "${current_lib_path%/*}/../")
    readonly bin_dir="${script_dir}"
    readonly root_dir="$(realpath ${lib_shared_dir}/../)"
    readonly module_dir="$(realpath ${script_dir}/../)"
    readonly shared_dir="${root_dir}/shared"
    readonly data_shared_dir="${shared_dir}/data"
    readonly raw_shared_dir="${data_shared_dir}/raw"
    readonly sql_shared_dir="${data_shared_dir}/sql"
    readonly conf_shared_dir="${shared_dir}/config"
    readonly conf_dir="${module_dir}/config"
    readonly data_dir="${module_dir}/data"
    readonly raw_dir="${data_dir}/raw"
    readonly sql_dir="${data_dir}/sql"
    readonly lib_dir="${module_dir}/lib"
    readonly var_dir="${module_dir}/var"
    readonly log_dir="${var_dir}/log"
    readonly tmp_dir="${var_dir}/tmp"

    #+----------------------------------------------------------------------------+
    # Shell colors
    readonly RCol="\e[0m";# Text Reset
    readonly Red="\e[1;31m"; # Text Dark Red
    readonly Gre="\e[1;32m"; # Text Dark Green
    readonly Yel="\e[1;33m"; # Text Yellow
    readonly Mag="\e[1;35m"; # Text Magenta
    readonly Gra="\e[1;30m"; # Text Dark Gray
    readonly Whi="\e[1;37m"; # Text Dark White
    readonly Std="\e[0m"; # Text normal
    readonly Blink="\e[5m"; #Text blink

    #+----------------------------------------------------------------------------+
    # Section separator
    readonly sep_limit=100
    readonly sep="$(printf "=%.0s" $(seq 1 ${sep_limit}))\n"

    #+----------------------------------------------------------------------------+
    # Important to always set as we use it in the exit handler
    readonly ta_none="$(tput sgr0 2> /dev/null || true)"
}

# DESC: Exit script with the given message
# ARGS: $1 (required): Message to print on exit
#       $2 (optional): Exit code (defaults to 0)
# OUTS: None
# NOTE: The convention used in this script for exit codes is:
#       0: Normal exit
#       1: Abnormal exit due to external error
#       2: Abnormal exit due to script error
# SOURCE: https://github.com/ralish/bash-script-template/blob/stable/source.sh
function exitScript() {
    if [[ $# -eq 1 ]]; then
        printf '%s\n' "${1}"
        exit 0
    fi

    if [[ ${2-} =~ ^[0-9]+$ ]]; then
        if [[ ${2} -ne 0 ]]; then
            printError "${1}"
        else
            printInfo "${1}"
        fi
        exit ${2}
    fi

    exitScript "Missing required argument to ${FUNCNAME[0]}()!" 2
}

# DESC: Pretty print the provided string
# ARGS: $1 (required): Message to print (defaults to a yellow)
#       $2 (optional): Colour to print the message with. This can be an ANSI
#                      escape code.
# OUTS: None
# SOURCE: https://github.com/ralish/bash-script-template/blob/stable/source.sh
function printPretty() {
    if [[ $# -lt 1 ]]; then
        exitScript "Missing required argument to ${FUNCNAME[0]}()!" 2
    fi

    if [[ -n ${2-} ]]; then
        echo -e "${2}${1}${RCol}"
    else
        echo -e "${Yel}${1}${RCol}"
    fi
}

# DESC: Print a section message
# ARGS: $1 (required): Message to print
# OUTS: None
function printMsg() {
    if [[ $# -lt 1 ]]; then
        exitScript "Missing required argument to ${FUNCNAME[0]}()!" 2
    fi
    printPretty "⮕ ${1}" ${Yel}
}

# DESC: Print infos message
# ARGS: $1 (required): Message to print
# OUTS: None
function printInfo() {
    if [[ $# -lt 1 ]]; then
        exitScript "Missing required argument to ${FUNCNAME[0]}()!" 2
    fi
    printPretty "⮕ ${1}" ${Whi}
}

# DESC: Print an error message
# ARGS: $1 (required): Message to print
# OUTS: None
function printError() {
    if [[ $# -lt 1 ]]; then
        exitScript "Missing required argument to ${FUNCNAME[0]}()!" 2
    fi
    printPretty "⮕ ${1}" ${Red}
}

# DESC: Only printPretty() the provided string if verbose mode is enabled
# ARGS: $@ (required): Passed through to printPretty() function
# OUTS: None
# SOURCE: https://github.com/ralish/bash-script-template/blob/stable/source.sh
function printVerbose() {
    if [[ -n ${verbose-} ]]; then
        if [[ $# -eq 1 ]]; then
            printPretty "${1}" ${Gra}
        else
            printPretty "${@}"
        fi
    fi
}

# DESC: Load shared default config file
# ARGS: None
# OUTS: All variables and constants from default config file.
function loadSharedDefaultConfig() {
    local default_setting_file_path=$(realpath "${conf_shared_dir}/settings.default.ini")
    if [[ -f "${default_setting_file_path}" ]] ; then
        source "${default_setting_file_path}"
        printVerbose "Loading SHARED  default settings '${default_setting_file_path}': ${Gre-}OK" ${Gra-}
    else
        printError "Config file '${default_setting_file_path}' not found."
        exitScript "Please restore default configuration file '${default_setting_file_path}' from source." 1
    fi
}

# DESC: Load user config file
# ARGS: $1 (optional): User config file path (default ${conf_shared_dir}/settings.ini)
# OUTS: All variables and constants from user config file.
function loadSharedUserConfig() {
    local default_user_setting_file_path=$(realpath "${conf_shared_dir}/settings.ini")
    local config_path=${1:-$default_user_setting_file_path}
    if [[ -f "${config_path}" ]] ; then
        source "${config_path}"
        printVerbose "Loading SHARED user settings '${config_path}': ${Gre-}OK" ${Gra-}
    else
        printVerbose "Optional SHARED user settings config file not found at '${config_path}'"
    fi
}

# DESC: Load shared default config file
# ARGS: None
# OUTS: All variables and constants from default config file.
function loadModuleDefaultConfig() {
    local default_setting_file_path=$(realpath "${conf_dir}/settings.default.ini")
    if [[ -f "${default_setting_file_path}" ]] ; then
        source "${default_setting_file_path}"
        printVerbose "Loading MODULE default settings '${default_setting_file_path}': ${Gre-}OK" ${Gra-}
    else
        printError "Config file '${default_setting_file_path}' not found."
        exitScript "Please restore default configuration file '${default_setting_file_path}' from source." 1
    fi
}

# DESC: Load user config file
# ARGS: $1 (optional): User config file path (default ${conf_dir}/settings.ini)
# OUTS: All variables and constants from user config file.
function loadModuleUserConfig() {
    local default_user_setting_file_path=$(realpath "${conf_dir}/settings.ini")
    local config_path=${1:-$default_user_setting_file_path}
    if [[ -f "${config_path}" ]] ; then
        source "${config_path}"
        printVerbose "Loading MODULE user settings '${config_path}': ${Gre-}OK" ${Gra-}
    else
        printVerbose "Optional MODULE user settings config file not found at '${config_path}'"
    fi
}

# DESC: Load all script config files in right order : default then user settings files
# ARGS: $1 (optional): module user config file path (default ${conf_dir}/settings.ini). Last loaded config file.
# OUTS: All variables and constants from default and user config file.
function loadScriptConfig() {
    loadSharedDefaultConfig
    loadSharedUserConfig
    loadModuleDefaultConfig
    loadModuleUserConfig "${1:-}"
}


# DESC: Redirect output
#       Send stdout and stderr in Terminal and a log file
#       In Terminal replace "⮕ " by empty string
#       In logfile replace "⮕ " by a separator line and remove color characters.
# ARGS: $1 (required): Log file path.
# OUTS: None
# NOTE: Directories on log file path will be create if not exist.
#       All lines with a carriage return "\r" will be removed from log file.
#       In script use :
#           `>&3` to redirect to original stdOut
#           `>&4` to redirect to original stdErr
# SOURCE: https://stackoverflow.com/a/20564208
redirectOutput() {
    if [[ $# -lt 1 ]]; then
        exitScript "Missing required argument to ${FUNCNAME[0]}()!" 2
    fi
    local log_file="${1}"
    local log_file_dir="$(dirname "${log_file}")"
    if [[ ! -d "${log_file_dir}" ]]; then
        printVerbose "Create log directory..."
        mkdir -p "${log_file_dir}"
    fi

    exec 3>&1 4>&2 1>&>(sed -r "s/⮕ //g") 1>&2>&>(tee -a >(grep -v $'\r' | sed -r 's/\x1B\[([0-9]{1,2}(;[0-9]{1,2})?)?[m|K]//g' | sed -r "s/⮕ /${sep}/g" > "$1"))
}

# DESC: Check a binary exists in the search path
# ARGS: $1 (required): Array of names of the binary to test for existence
#       $2 (optional): Set to any value to treat failure as a fatal error
# OUTS: None
# SOURCE: https://github.com/ralish/bash-script-template/blob/stable/source.sh
function checkBinary() {
    if [[ $# -lt 1 ]]; then
        exitScript "Missing required argument to ${FUNCNAME[0]}()!" 2
    fi
    commands=("${@}")
    for cmd in "${commands[@]}"; do
        if ! command -v "${cmd}" > /dev/null 2>&1; then
            if [[ -n ${2-} ]]; then
                exitScript "Missing dependency: Couldn't locate: ${cmd}" 1
            else
                printError "Missing dependency: ${cmd}"
                return 1
            fi
        fi
        printVerbose "Found dependency: ${cmd} => ${Gre-}OK" ${Gra-}
    done
    return 0
}

# DESC: Validate we have superuser access as root (via sudo if requested)
# ARGS: $1 (optional): Set to any value to not attempt root access via sudo
# OUTS: None
# SOURCE: https://github.com/ralish/bash-script-template/blob/stable/source.sh
function checkSuperuser() {
    local superuser
    if [[ ${EUID} -eq 0 ]]; then
        superuser=true
    elif [[ -z ${1-} ]]; then
        if checkBinary "sudo"; then
            printVerbose 'Sudo: Updating cached credentials ...'
            if ! sudo -v; then
                printVerbose "Sudo: Couldn't acquire credentials ..." "${Red-}"
            else
                local test_euid
                test_euid="$(sudo -H -- "${BASH}" -c 'printf "%s" "${EUID}"')"
                if [[ ${test_euid} -eq 0 ]]; then
                    superuser=true
                fi
            fi
        fi
    fi

    if [[ -z ${superuser-} ]]; then
        printVerbose 'Unable to acquire superuser credentials.' "${Red-}"
        return 1
    fi

    printVerbose 'Successfully acquired superuser credentials.'
    return 0
}

# DESC: Show time elapsed
# ARGS: None
# OUTS: None
# NOTE: Use 'time_start' variable define in initScript() function.
function displayTimeElapsed() {
    local time_end="$(date +%s)"
    local time_diff="$((${time_end} - ${time_start}))"
    printInfo "Total time elapsed: $(displayTime "${time_diff}")"
}

# DESC: Display seconds in days, hours, minutes, rest seconds
# ARGS: $1 (required): Number of seconds
# OUTS: None
# SOURCE: https://unix.stackexchange.com/a/27014
function displayTime() {
    if [[ $# -lt 1 ]]; then
        exitScript "Missing required argument to ${FUNCNAME[0]}()!" 2
    fi
    local T="${1}"
    local D=$((T/60/60/24))
    local H=$((T/60/60%24))
    local M=$((T/60%60))
    local S=$((T%60))
    [[ $D > 0 ]] && printf '%d days ' $D
    [[ $H > 0 ]] && printf '%d hours ' $H
    [[ $M > 0 ]] && printf '%d minutes ' $M
    [[ $D > 0 || $H > 0 || $M > 0 ]] && printf 'and '
    printf '%d seconds\n' $S
}

# DESC: Draw a progress bar
# ARGS: $1 (required): number of total tasks
#       $2 (required): actual number of tasks done
#       $3 (optional): text to display after progress bar (default: 'in progress')
#       $4 (optional): number of characters for progress bar width (default: 20)
# OUTS: None
# SOURCE: https://gist.github.com/F1LT3R/fa7f102b08a514f2c535
function displayProgressBar() {
    if [[ $# -lt 2 ]]; then
        exitScript "Missing required argument to ${FUNCNAME[0]}()!" 2
    fi

    local task_count="${1}"
    local tasks_done="${2}"
    local text="${3:-in progress}"
    local progress_bar_width=${4:-20}

    # Calculate number of fill/empty slots in the bar
    local progress="$(echo "${progress_bar_width} / ${task_count} * ${tasks_done}" | bc -l)  "
    local fill="${progress%.*}"
    if [[ ${fill} -gt ${progress_bar_width} ]]; then
        local fill=${progress_bar_width}
    fi
    local empty="$((${fill} - ${progress_bar_width}))"

    # Percentage Calculation
    local percent="$(echo "100 / ${task_count} * ${tasks_done}" | bc -l)"
    # Check if percent is greater than 0 based on string because
    # number like ".98808030112923462933" rase an error "(standard_in) 1: syntax error"
    if [[ "${percent}" == .* ]]; then
        local percent="0"
    else
        local percent="${percent%.*}"
        if [[ $(echo "${percent} > 100" | bc -l) -gt 0 ]]; then
            local percent="100"
        fi
    fi

    # Output to screen
    printf "\r[" >&3
    printf "%${fill}s" '' | tr ' ' "=" >&3
    printf "%${empty}s" '' | tr ' ' "." >&3
    printf "] ${percent}%% - ${text} " >&3
}

# DESC: Trim spaces at beginning and ending of a string
# ARGS: $1 (required): string to trim spaces
# OUTS: None
# SOURCE:
trim() {
    if [[ $# -lt 1 ]]; then
        exitScript "Missing required argument to ${FUNCNAME[0]}()!" 2
    fi
    local var="$*"
    # remove leading whitespace characters
    var="${var#"${var%%[![:space:]]*}"}"
    # remove trailing whitespace characters
    var="${var%"${var##*[![:space:]]}"}"
    printf '%s' "$var"
}

# DESC: Download file with Wget
# ARGS: $1 (required): URL where download the file
# ARGS: $2 (required): path of file where download it locally
# OUTS: None
# SOURCE: -
function downloadWeb() {
    if [[ $# -lt 2 ]]; then
        exitScript "Missing required argument to ${FUNCNAME[0]}()!" 2
    fi
    local readonly commands=("wget" "grep" "uniq")
    checkBinary "${commands[@]}"

    local readonly url="${1}"
    local readonly file="${2}"
    wget --no-check-certificate --progress=dot "${url}" -O "${file}" 2>&1 | grep --line-buffered -E -o "100%|[1-9]0%|^[^%]+$" | uniq
    printVerbose "Download ${file}: ${Gre}DONE${RCol}"
}

# DESC: Download file with Sftp
# ARGS: $1 (required): user name
# ARGS: $2 (required): user password
# ARGS: $3 (required): sftp host server IP address
# ARGS: $4 (required): sftp host server if not equal to 22
# ARGS: $5 (required): remote file path to dowload
# ARGS: $6 (required): local file path for the downloaded file
# OUTS: None
# SOURCE: -
function downloadSftp() {
    if [[ $# -lt 6 ]]; then
        exitScript "Missing required argument to ${FUNCNAME[0]}()!" 6
    fi
    local readonly commands=("sshpass" "sftp")
    checkBinary "${commands[@]}"

    local readonly user="${1}"
    local readonly pwd="${2:-22}"
    local readonly host="${3}"
    local readonly port="${4}"
    local readonly remote_file="${5}"
    local readonly local_file="${6}"
    sshpass -p "${pwd}" sftp -oStrictHostKeyChecking=no -oPort="${port}" \
        "${user}@${host}:${remote_file}" "${local_file}"

    if [[ -f "${local_file}" ]]; then
        printVerbose "SFTP download ${local_file}: ${Gre}DONE${RCol}"
    else
        printVerbose "SFTP download: ${Red}something wrong with ${remote_file} ${RCol}"
    fi
}

# DESC: Upload file with Sftp
# ARGS: $1 (required): user name
# ARGS: $2 (required): user password
# ARGS: $3 (required): sftp host server IP address
# ARGS: $4 (required): sftp host server if not equal to 22
# ARGS: $5 (required): path of remote directory where to upload the file
# ARGS: $6 (required): path of local file to upload
# OUTS: None
# SOURCE: -
function uploadSftp() {
    if [[ $# -lt 6 ]]; then
        exitScript 'Missing required argument to upload()!' 6
    fi
    local readonly commands=("sshpass" "sftp")
    checkBinary "${commands[@]}"

    local readonly user="${1}"
    local readonly pwd="${2:-22}"
    local readonly host="${3}"
    local readonly port="${4}"
    local readonly remote_dir="${5}"
    local readonly local_file="${6}"
    sshpass -p "${pwd}" sftp -oStrictHostKeyChecking=no -oPort="${port}" \
        "${user}@${host}:${remote_dir}" <<< "put ${local_file}"

    if [[ -f "${local_file}" ]]; then
        printVerbose "${Gra}SFTP upload ${local_file}: ${Gre}DONE${RCol}"
    else
        printVerbose "${Gra}SFTP upload: ${Red}something wrong with ${remote_file} ${RCol}"
    fi
}

# DESC: Extract archive file
# ARGS: $1 (required): Archive file to extract.
# OUTS: None
# SOURCE: https://ostechnix.com/a-bash-function-to-extract-file-archives-of-various-types/
function extract() {
    if [[ $# -lt 1 ]]; then
        exitScript "Missing required argument to ${FUNCNAME[0]}()!" 2
    fi

    if [[ -f $1 ]] ; then
        case $1 in
            *.tar.bz2) tar xvjf $1 ;;
            *.tar.gz) tar xvzf $1 ;;
            *.bz2) bunzip2 $1 ;;
            *.rar) rar x $1 ;;
            *.gz) gunzip $1 ;;
            *.tar) tar xvf $1 ;;
            *.tbz2) tar xvjf $1 ;;
            *.tgz) tar xvzf $1 ;;
            *.zip) unzip $1 ;;
            *.Z) uncompress $1 ;;
            *.7z) 7z x $1 ;;
            *) printError "Archive format unknown: '$1'" ;;
        esac
    else
        printError "'$1' is not a valid file!"
    fi
}


# DESC: send a message to Telgram group.
#       Variables telegram_group_id and telegram_bot_token
#       must be set in a settings.ini file.
#       Use telegram_url variable optionally.
# ARGS: $1 (required): message to send.
# OUTS: None
# SOURCE: https://bogomolov.tech/Telegram-notification-on-SSH-login/
function sendTelegram() {
    if [[ -z "${telegram_bot_token-}" ]] || [[ -z "${telegram_group_id-}" ]]; then
        exitScript "Please define TELEGRAM_BOT_TOKEN and TELEGRAM_GROUP_ID (TELEGRAM_URL optionally) before used of this function."
    fi

    if [[ -z "${telegram_url-}" ]]; then
        telegram_url="https://api.telegram.org/bot${telegram_bot_token}/sendMessage"
    fi

    if [[ "$1" == "-h" ]]; then
        exitScript "Usage: ${FUNCNAME[0]} \"text message\""
    fi

    if [[ -z "$1" ]]; then
        exitScript "Missing required argument (message text) to ${FUNCNAME[0]}()!"
    fi

    if [[ "$#" -ne 1 ]]; then
        exitScript "You can pass only one argument to ${FUNCNAME[0]}(). For string with spaces put it on quotes."
    fi

    # Send message
    if [[ -z "${telegram_verbose-}" ]]; then
        curl --silent --show-error --data "text=$1" --data "chat_id=${telegram_group_id}" "${telegram_url}" > /dev/null
    else
        curl --silent --show-error --data "text=$1" --data "chat_id=${telegram_group_id}" "${telegram_url}"
    fi
}

# DESC: Ask to execute next step (=function), jump or cancel script (=exit).
# ARGS: $1 (required): function containing next step.
# OUTS: None
# SOURCE:
function stepToNext() {
    if [[ $# -lt 1 ]]; then
        exitScript "Missing required argument to ${FUNCNAME[0]}()!" 2
    fi

    function_name="${1-}"
    step=${step:=0}
    step=$((step + 1))

    if [[ "${step}" = "1" ]]; then
        printPretty "${Blink}HELP:${RCol} ${Mag}y=yes, j=jump (not execute next function), c=cancel script (exit)" ${Mag}
    fi

    echo # Move to a new line
    printPretty "Step #${step} (⌚ $(date +'%H:%M'))- Go to next step '${function_name}' (y/j/c) ?" ${Mag}
    read -r -n 1 key
    echo # Move to a new line
    if [[ ! "${key}" =~ ^[YyjJ]$ ]];then
        printPretty "Are you sure to exit script (y/n) ?" ${Red}
        read -r -n 1 key
        echo # Move to a new line
        if [[ "${key}" =~ ^[Yy]$ ]];then
            [[ "${0}" = "${BASH_SOURCE}" ]] && exit 1 || return 1 # handle exits from shell or function but don't exit interactive shell
        fi
    fi
    if [[ ! "${key}" =~ ^[Jj]$ ]];then
        "$@"
    fi
}

# DESC: Return true if first argument semantic version string is greater than second.
# ARGS: $1 (required): first version string
# ARGS: $2 (required): second version string
# OUTS: None
# SOURCE:
function isVersionGreaterThan() {
	test "$(printf '%s\n' "$@" | sort -V | head -n 1)" != "$1";
}
