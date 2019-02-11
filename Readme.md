# Spark Tutorial

This is a basic scala project that includes spark dependencies and necessary build-configuration to build a jar that can be submitted to a spark installation via spark-submit.

Things to do after checking out and opening in intellij:

- Add scala SDK 2.11.11 (tested with this version, it has to be 2.11.x since this is the spark version)
- Add jdk 8 (**very important to use version 8** as this is required by spark)
- add Framework support of Maven

To build the project run `mvn clean package`

## Spark Standalone Cluster using Docker

This project comes with a small wrapper script to run a [standalone spark cluster](https://spark.apache.org/docs/latest/spark-standalone.html) with [Docker](https://docs.docker.com/) on your own machine.
It uses the docker images published by [actionml](https://hub.docker.com/u/actionml/) and is written in [Bash](https://www.gnu.org/software/bash/) and should therefore work on Mac and Linux systems out-of-the-box.

The script was tested on the following systems:

- Ubuntu 18.10 (kernel 4.18.0-15) with Docker version 18.06.1-ce

### Requirements

- A Bash compatible shell
- The docker CLI and a running docker daemon. See the [docker documentation](https://docs.docker.com/install/) on how to install docker.

### Usage

1. Build your Spark job as a jar using the following command:
   ```shell
   mvn clean package -DskipTests
   ```
2. Bring up the cluster using the wrapper script.
   **Start the cluster from the project's root directory!**
   This is necessary as all cluster nodes need access to the data and the job's jar file.
   The script therefore mounts the current working directory into the node's containers.
   ```shell
   ./spark-cluster.sh start -n 2
   ```
   This starts the master node and two worker nodes, all in their own Docker containers, and connects them.
   You can connect to the Spark web UI of the master using the URL [http://localhost:8080](http://localhost:8080) (if you use the default configuration for the ports).
   The workers' UI can be accessed using ascending port numbers.
3. Submit the jar as a job to the cluster.
   Please be aware that overriding the configuration of Spark for example with `new SparkContext(new SparkConf().master("local[4]"))` will take precedence over other supplied configuration arguments.
   The mentioned example will run the Spark job **not** in the cluster, but just locally in the docker container submitting the job (_Spark driver process_).
   The wrapper script will take care of setting the URL to the master node correctly.
   ```sh
   ./spark-cluster.sh submit --class my.package.MainClass sum.jar 5 100
   ```
   Program arguments and the main class definition are optional.
4. Stop the cluster and remove the Docker containers with the following command:
   ```sh
   ./spark-cluster.sh stop
   ```

### Known Problems

- One can only use the script in the project's root folder.