#!/usr/bin/env bash

set -e

# read configuration and prepare volumes
: ${SPARK_HOME:?must be set!}
. ${SPARK_HOME}/sbin/spark-config.sh
. ${SPARK_HOME}/bin/load-spark-env.sh

# run submit command
cd /work
exec ${SPARK_HOME}/bin/spark-submit $@