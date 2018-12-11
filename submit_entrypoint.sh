#!/usr/bin/env bash

set -e

# read default opts
: ${SPARK_HOME:?must be set!}
default_opts="--properties-file /spark-defaults.conf"

# read configuration and prepare volumes
. ${SPARK_HOME}/sbin/spark-config.sh
. ${SPARK_HOME}/bin/load-spark-env.sh
mkdir -p ${SPARK_HOME}/work && chown ${SPARK_USER}:hadoop ${SPARK_HOME}/work
chmod 1777 /tmp

# run submit command
cd /work
exec gosu ${SPARK_USER}:hadoop ${SPARK_HOME}/bin/spark-submit ${default_opts} $@