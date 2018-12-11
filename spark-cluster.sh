#!/usr/bin/env bash

# prints the help information using `cat` and a HEREDOC
function printHelp() {
cat <<HELP
This script eases handling of the spark cluster using docker.
The docker images are from https://github.com/actionml/docker-spark.
Usage: $0 COMMAND

Commands:
    COMMAND use '$0 COMMAND -h' to print help for each command
    start   starts the spark cluster using docker run
    stop    stops all started spark containers
    submit  submit a job to the spark cluster
    shell   runs a spark shell for interactive command processing inside spark
    help    prints this help page


Example:
> $0 start -n 2
HELP
}

function printStopHelp() {
cat <<STOPHELP
Stops all docker containers used by the spark cluster.
Usage: $0 stop

Example:
> $0 stop
STOPHELP
}

# constants
namefile=".workernames"
master_name="spark-master"
worker_name="spark-worker"
default_n=2

function start() {
    # set getopts counter
    old_optind=$OPTIND
    OPTIND=2

    n=${default_n}
    while getopts "h?n:" option; do
      case "${option}" in
        n) n=${OPTARG} ;;
        h|\?) printHelp
            exit 1 ;;
      esac
    done

    # reset getopts counter
    OPTIND=${old_optind}

    # Spawn master
    docker run --rm -d --name ${master_name} -p 8080:8080 actionml/spark master
    master_ip=$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' spark-master)

    # Spawn workers
    counter=0
    while [[ ${counter} -lt ${n} ]]; do
        echo "${worker_name}${counter}" >> ${namefile}
        docker run --rm -d --name "${worker_name}${counter}" actionml/spark worker spark://${master_ip}:7077
        let counter=counter+1
    done
}

function stop() {
    # set getopts counter
    old_optind=$OPTIND
    OPTIND=2

    while getopts "h" option; do
      case "${option}" in
        h|\?) printStopHelp
            exit 1 ;;
      esac
    done

    # reset getopts counter
    OPTIND=${old_optind}

    if [[ "$2" = "help" ]]; then
        printStopHelp
        exit 0
    fi

    # test if there are containers running
    if ! [[ -e ${namefile} ]]; then
        echo "No spark container running"
        exit 0
    fi

    echo "stopping master"
    docker stop ${master_name} 1>/dev/null 2>1
    for worker in $( cat ${namefile} ); do
        echo "stopping ${worker}"
        docker stop ${worker} 1>/dev/null 2>1
    done
    rm ${namefile}
}

function shell() {
    # Spawn shell
    docker run --rm -it actionml/spark shell --master spark://${master_ip}:7077
}


# ---------------------------------------------------------
# main
# ---------------------------------------------------------
# match commands
command=$1
if [[ "$command" = "start" ]]; then
    start "$@"
elif [[ "$command" = "stop" ]]; then
    stop "$@"
elif [[ "$command" = "shell" ]]; then
    shell "$@"
else
    printHelp
    exit 1
fi