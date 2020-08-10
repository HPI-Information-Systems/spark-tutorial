This is a basic scala project that includes spark dependencies and necessary build-configuration to build a jar that can be submitted to a spark installation via spark-submit. In order to run this, you need to
- install sbt
- add scala plugin to your IDE of choice (in the following we describe this for Intellij)
- check out the project and open it in your IDE
- modify the settings in the build.sbt to match the spark installation (Relevant settings are both the **scalaVersion** parameter, as well as the specific versions of all the spark packages (for example in: **libraryDependencies += "org.apache.spark" %% "spark-core" % "x"** <-- replace x with the version of the spark installation, where you want to execute the code). If not both the scala version as well as the spark version exactly match those of the spark installation you will encounter errors while executing the jar that are not really helpful in determining the cause.
- Intellij should automatically recognize this as an sbt project (initialization, for example indexing files might take a while)
- If Intellij does not automatically download the depencencies, you can either open the console and type **sbt compile** or open the sbt tab (right edge of the screen) and click "Reload all sbt projects"
- Now you should be able to execute the code in Intellij
- Run **sbt clean assembly** in your console to build a Fat-jar for deployment 
