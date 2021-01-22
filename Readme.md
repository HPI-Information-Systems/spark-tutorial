Dies ist ein scala project, welches bereits alle Apache-Spark (https://spark.apache.org/) Dependencies und Build-Konfigurationen enthält um eine jar Datei zu bauen, welche auf einem Cluster ausgeführt werden kann.
Für die DBS II Übung müsst ihr den Code aber lediglich lokal bei euch ausführen können. 
Dafür müsst ihr folgendes tun:
- installiert sbt (https://www.scala-sbt.org/)
- Fügt in eurer IDE ein Scala-Plugin hinzu, sodass ihr Scala-Code kompilieren könnt (im folgenden gehen wir von Intellij als IDE aus: https://www.jetbrains.com/idea/)
- Ladet das Projekt herunter und öffnet es in der IDE
- Intellij sollte dieses nun automatisch als ein sbt Project erkennen (die Initialisierung, zum Beispiel das Indizieren der Dateien kann kurz dauern)
- Falls intellij die Dependencies nicht automatisch herunterlädt könnt ihr diese entweder herunterladen durch
  - Öffnen der Console (alt+F12) und das Kommando **sbt compile**
  - Öffnen des SBT-Tabs (rechter Rand des Bildschirms) und clicken auf "Reload all sbt projects"
- Nun solltet ihr sowohl **de.hpi.getting_started.ScalaIntroduction**, **de.hpi.getting_started.SparkIntroduction**, als auch **de.hpi.dbsII_exercises.DBSIISparkExerciseMain** ausführen können.

Für die 4 zu lösenden Teilaufgaben findet ihr eigens angelegte und entsprechend benannte Klassen im Package [de.hpi.dbsII_exercises](). Das Main-Object [DBSIISparkExerciseMain](de.hpi.dbsII_exercises.DBSIISparkExerciseMain) ist der Einstiegspunkt in das Programm, startet Spark und liest die Daten ein und ruft die zu implementierenden Methoden auf.
Dieses erwartet 2 Parameter. Der erste ist der Ordner, in dem sich die Input-Daten befinden (diese findet ihr [hier](https://hpi.de/fileadmin/user_upload/fachgebiete/naumann/lehre/WS2020/DBS_II/Test_Input_Spark.zip)). Der zweite ist die Anzahl an Kernen mit denen Spark lokal ausgeführt werden soll. Hier solltet ihr maximal die Anzahl eurer CPU-Kerne angeben.
Nachdem die von euch zu implementierenden Methoden aufgerufen wurden, werden die von euch zurückgegebenen Resultate auf Korrektheit überprüft und entsprechendes wird auf der Konsole ausgegeben. Die erwarteten Resultate könnt ihr übrigens unter [hier](../data/) einsehen. 

If you want to execute your spark code on a spark cluster (not relevant for this exercise):
- Modify the settings in the build.sbt to match the spark installation (Relevant settings are both the **scalaVersion** parameter, as well as the specific versions of all the spark packages (for example in: **libraryDependencies += "org.apache.spark" %% "spark-core" % "x"** <-- replace x with the version of the spark installation, where you want to execute the code). If not both the scala version as well as the spark version exactly match those of the spark installation you will encounter errors while executing the jar that are not really helpful in determining the cause.
- Run **sbt clean assembly** in your console to build a Fat-jar for deployment 
