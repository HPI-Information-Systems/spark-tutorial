#!/usr/bin/env bash


# spark-cluster.sh
# CLI wrapper script to use a spark cluster based on docker images.
# The docker images are build from https://github.com/actionml/docker-spark
# and hosted on dockerhub. They are pulled automatically.
#
# Author: Sebastian Schmidl <info at sebastianschmidl dot de>

# prints the help information using `cat` and a HEREDOC
function printHelp() {
cat <<HELP
This script eases handling of the spark cluster using docker.
The docker images are from https://github.com/actionml/docker-spark.
Usage: $0 COMMAND [command options]

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

Options:
    -h      Prints this help page

Example:
> $0 stop
STOPHELP
}

function printStartHelp() {
cat <<STARTHELP
Starts a spark master docker container and N worker docker containers.
Usage: $0 start -n N -p PORT

Options:
    -h      Prints this help page
    -n N    Number of worker nodes to start, defaults to 2
    -p PORT Spark WebUI port on the host system, defaults to 8080

Example:
> $0 start
STARTHELP
}

function printShellHelp() {
cat <<SHELLHELP
Runs the interactive spark shell on the already started cluster.
Usage: $0 shell

Options:
    -h      Prints this help page

Example:
> $0 shell
SHELLHELP
}

function printSubmitHelp() {
cat <<SUBMITHELP
Submits a jar-file as a spark job to the spark cluster for processing.
Usage: $0 submit [OPTIONS] JAR [JAR_ARGS]

Options:
    -h      Prints this help page

Example:
> $0 submit --class my.package.MainClass sum.jar 5 100
SUBMITHELP
}

# constants
namefile=".workernames"
master_name="spark-master"
worker_name="spark-worker"
default_n=2
default_webui_port=8080

function start() {
    # set getopts counter
    old_optind=$OPTIND
    OPTIND=2

    n=${default_n}
    p=${default_webui_port}
    while getopts "h?n:p:" option; do
      case "${option}" in
        n) n=${OPTARG} ;;
        p) p=${OPTARG} ;;
        h|\?) printStartHelp
            exit 1 ;;
      esac
    done

    # reset getopts counter
    OPTIND=${old_optind}

    # Spawn master
    docker run --rm -d --name ${master_name} -p ${p}:8080 actionml/spark master >/dev/null
    echo "${master_name} started"
    master_ip=$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' ${master_name})

    # Spawn workers
    counter=0
    while [[ ${counter} -lt ${n} ]]; do
        echo "${worker_name}${counter}" >> ${namefile}
        docker run --rm -d --name "${worker_name}${counter}" actionml/spark worker spark://${master_ip}:7077 >/dev/null
        echo "${worker_name}${counter} started"
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

    if [[ "$2" != "" ]]; then
        printStopHelp
        exit 1
    fi

    # test if there are containers running using the namefile
    if ! [[ -e ${namefile} ]]; then
        echo "No spark container running"
        exit 0
    fi

    echo "stopping master"
    docker stop ${master_name} 1>/dev/null 2>&1
    for worker in $( cat ${namefile} ); do
        echo "stopping ${worker}"
        docker stop ${worker} 1>/dev/null 2>&1
    done
    rm ${namefile}
}

function shell() {
    # set getopts counter
    old_optind=$OPTIND
    OPTIND=2

    while getopts "h" option; do
      case "${option}" in
        h|\?) printShellHelp
            exit 1 ;;
      esac
    done

    # reset getopts counter
    OPTIND=${old_optind}


    if [[ "$2" != "" ]]; then
        printShellHelp
        exit 1
    fi

    # test if master is running
    is_running=$(docker inspect -f "{{.State.Running}}" ${master_name} 2>/dev/null)
    if ! [[ "${is_running}" = true ]]; then
        echo "The cluster is not running. Use '$0 start' to start the spark cluster with the default configuration."
        exit 1
    fi

    # Spawn shell
    master_ip=$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' ${master_name})
    docker run --rm -it actionml/spark shell --master spark://${master_ip}:7077
}

function submit() {
    # set getopts counter
    old_optind=$OPTIND
    OPTIND=2

    if [[ "$2" = "" ]]; then
        echo "You must at least specify a jar file!"
        printSubmitHelp
        exit 1
    elif [[ "$2" = "-h" ]]; then
        printSubmitHelp
        exit 1
    fi

    # test if master is running
    is_running=$(docker inspect -f "{{.State.Running}}" ${master_name} 2>/dev/null)
    if ! [[ "${is_running}" = true ]]; then
        echo "The cluster is not running. Use '$0 start' to start the spark cluster with the default configuration."
        exit 1
    fi

    # execute submission
    master_ip=$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' ${master_name})
    # docker run --rm -it -v $(pwd):/work actionml/spark /work/submit_entrypoint.sh --master spark://172.17.0.2:7077 --deploy-mode client --class de.hpi.spark_tutorial.SimpleSpark$ SparkTutorial-1.0.jar
    docker run --rm -it -v $(pwd):/tmp/work:ro actionml/spark /tmp/work/submit_entrypoint.sh --master spark://${master_ip}:7077 --deploy-mode cluster "${@:2}"

    # reset getopts counter
    OPTIND=${old_optind}
}


# ---------------------------------------------------------
# main
# ---------------------------------------------------------
# match commands
command=$1
if [[ "$command" = "start" ]]; then
    start $@
elif [[ "$command" = "stop" ]]; then
    stop $@
elif [[ "$command" = "shell" ]]; then
    shell $@
elif [[ "$command" = "submit" ]]; then
    submit $@
else
    printHelp
    exit 1
fi