This is a basic scala project that includes spark dependencies and necessary build-configuration to build a jar that can be submitted to a spark installation via spark-submit. In order to run this, you need to
- install sbt
- add scala plugin to your IDE of choice (in the following we describe this for Intellij)
- check out the project and open it in your IDE
- Intellij should automatically recognize this as an sbt project (initialization, for example indexing files might take a while)
- If Intellij does not automatically download the depencencies, you can either open the console and type **sbt compile** or open the sbt tab (right edge of the screen) and click "Reload all sbt projects"
- Now you should be able to execute the code in Intellij
- Run **sbt clean assembly** in your console to build a Fat-jar for deployment 
