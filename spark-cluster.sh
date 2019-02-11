#!/usr/bin/env bash

# spark-cluster.sh
# CLI wrapper script to use a spark cluster based on docker images.
# The docker images are build from https://github.com/actionml/docker-spark
# and hosted on dockerhub. They are pulled automatically.
#
# Author: Sebastian Schmidl <info at sebastianschmidl dot de>


# constants
image_version="2.3.2"
namefile=".workernames"
master_name="spark-master"
worker_name="spark-worker"
network_name="spark-cluster-network"
default_n=2
default_webui_port=8080
default_app_port=4040

# help functions
# print the help information using `cat` and a HEREDOC
function printHelp() {
cat <<HELP
This script eases handling of the spark cluster using docker. Please use this script only
from the project's root directory; It will be mounted into the master and worker docker
containers to let them access your jar and data files. The docker images are from
https://github.com/actionml/docker-spark.

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
    -n N    Number of worker nodes to start, defaults to ${default_n}
    -p PORT Start of port range for Spark WebUI ports on the host system.
            Will allocate to ports PORT to (PORT + N), PORT defaults to ${default_webui_port}.

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
    -p PORT Port of the Spark application WebUI on the host system.
            PORT defaults to ${default_app_port}

Example:
> $0 shell
SHELLHELP
}

function printSubmitHelp() {
cat <<SUBMITHELP
Submits a jar-file as a spark job to the spark cluster for processing. All paths are relative
to the current directory as this directory will be made available to the cluster.
Usage: $0 submit [OPTIONS] JAR [JAR_ARGS]

Options:
    -h      Prints this help page

Example:
> $0 submit --class my.package.MainClass sum.jar 5 100
SUBMITHELP
}

# commands
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

    if [[ "$2" != "" ]]; then
        echo "Unknown argument $2" >&2
        printStartHelp
        exit 1
    fi

    # Setup user-defined bridge network for service discovery
    docker network inspect ${network_name} 1>/dev/null 2>&1
    if [[ $? != 0 ]]; then
        docker network create --driver bridge "${network_name}" >/dev/null
        echo "Docker network ${network_name} created"
    fi

    # test if master is running
    is_running=$(docker inspect -f "{{.State.Running}}" ${master_name} 2>/dev/null)
    if [[ "${is_running}" = true ]]; then
        echo "The cluster is already running."
        exit 0
    fi

    # Spawn master
    docker run --rm -d --name "${master_name}" \
               --hostname "${master_name}" \
               --network "${network_name}" \
               -p ${p}:8080 \
               -v $(pwd):/work:ro \
               -v $(pwd)/spark/spark-env.sh:/spark/conf/spark-env.sh \
               -e SPARK_PUBLIC_DNS="localhost" \
               -e SPARK_HOSTNAME="${master_name}" \
               actionml/spark:${image_version} master >/dev/null 2>&1
    echo "${master_name} started"

    # Spawn workers
    counter=0
    while [[ ${counter} -lt ${n} ]]; do
        worker="${worker_name}${counter}"
        port=$((p + 1 + counter))
        docker run --rm -d --name "${worker}" \
                   --hostname "${worker}" \
                   --network "${network_name}" \
                   -p ${port}:${port} \
                   -v $(pwd):/work:ro \
                   -v $(pwd)/spark/spark-env.sh:/spark/conf/spark-env.sh \
                   -e SPARK_PUBLIC_DNS="localhost" \
                   -e SPARK_HOSTNAME="${worker}" \
                   actionml/spark:${image_version} worker spark://${master_name}:7077 --webui-port ${port} >/dev/null
        if [[ $? == 0 ]]; then
            echo "${worker}" >> ${namefile}
            echo "${worker} started"
        fi
        let counter=counter+1
    done

    echo "Web UI of the Spark master is available under: http://localhost:${p}"
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
        echo "Unknown argument $2" >&2
        printStopHelp
        exit 1
    fi

    # test if there are containers running using the namefile
    if [[ -e ${namefile} ]]; then
        echo "stopping master"
        docker stop ${master_name} 1>/dev/null 2>&1
        for worker in $( cat ${namefile} ); do
            echo "stopping ${worker}"
            docker stop ${worker} 1>/dev/null 2>&1
        done
        rm ${namefile}
    else
        echo "No spark container running"
    fi

    docker network inspect ${network_name} 1>/dev/null 2>&1
    if [[ $? == 0 ]]; then
        echo "Removing network"
        docker network rm "${network_name}"
    fi
}

function shell() {
    # set getopts counter
    old_optind=$OPTIND
    OPTIND=2

    p=${default_app_port}
    while getopts "h" option; do
      case "${option}" in
        p) p=${OPTARG} ;;
        h|\?) printShellHelp
            exit 1 ;;
      esac
    done

    # reset getopts counter
    OPTIND=${old_optind}


    if [[ "$2" != "" ]]; then
        echo "Unknown argument $2" >&2
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
    docker run --rm -it --name "spark-shell" \
               --network "${network_name}" \
               --hostname "spark-shell" \
               -v $(pwd):/work:ro \
               -p ${p}:4040 \
               -e SPARK_PUBLIC_DNS="localhost" \
               actionml/spark:${image_version} shell --master spark://${master_name}:7077 --conf "spark.driver.cores=1" --conf "spark.driver.memory=1g"
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
        exit 0
    fi

    # test if master is running
    is_running=$(docker inspect -f "{{.State.Running}}" ${master_name} 2>/dev/null)
    if ! [[ "${is_running}" = true ]]; then
        echo "The cluster is not running. Use '$0 start' to start the spark cluster with the default configuration."
        exit 1
    fi

    # execute submission
    docker run  --rm -it --name "spark-submitter" \
                --network "${network_name}" \
                --hostname "spark-submitter" \
                -v $(pwd):/work:ro \
                -p ${default_app_port}:4040 \
                -e SPARK_PUBLIC_DNS="localhost" \
                actionml/spark:${image_version} /work/spark/submit_entrypoint.sh \
                    --master spark://${master_name}:7077 \
                    --deploy-mode client \
                    "${@:2}"

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