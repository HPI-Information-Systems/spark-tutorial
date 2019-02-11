#!/usr/bin/env bash

# This file is sourced when running various Spark programs.

# spark binds to this IP
#SPARK_LOCAL_IP="0.0.0.0"
# public dns name of the driver program
#SPARK_PUBLIC_DNS="localhost"

#SPARK_MASTER_OPTS="-Dspark.deploy.defaultCores=2"
SPARK_WORKER_CORES=2
SPARK_WORKER_MEMORY=4g
#SPARK_WORKER_OPTS="-Dspark.worker.cleanup.enabled=true -Dspark.worker.cleanup.appDataTtl=3600"