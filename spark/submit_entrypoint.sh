#!/usr/bin/env bash

# submit_entrypoint.sh
# Script used in the docker container for submitting the Spark jobs to the cluster.
# This is part of `spark-cluster.sh`.
#
# Author: Sebastian Schmidl <info at sebastianschmidl dot de>

set -e

# read configuration and prepare volumes
: ${SPARK_HOME:?must be set!}
. ${SPARK_HOME}/sbin/spark-config.sh
. ${SPARK_HOME}/bin/load-spark-env.sh

# run submit command
cd /work
exec ${SPARK_HOME}/bin/spark-submit $@