package de.hpi.dbsII_exercises

import org.apache.spark.sql.SparkSession

object DBSIISparkExerciseMain extends App{

  val pathToData = args(0)
  val numCores = args(1).toInt
  val numShufflePartitions = 8

  val sparkBuilder = SparkSession
    .builder()
    .appName("SparkTutorial")
    .master(s"local[$numCores]") // local, with 4 worker cores
  val spark = sparkBuilder.getOrCreate()
  import spark.implicits._

  // Set the default number of shuffle partitions (default is 200, which is too high for local deployment)
  spark.conf.set("spark.sql.shuffle.partitions", 8) //

  val changeRecords = IOHelper.readSparkCSV(spark,pathToData,true)
    .map(r => ChangeRecord.from(r))
    .cache()

  val res3a = new Exercise_3a(spark,changeRecords).execute()
  val res3b = new Exercise_3b(spark,changeRecords).execute()
  val res3c = new Exercise_3c(spark,changeRecords).execute()
  val res3d = new Exercise_3d(spark,changeRecords).execute()

  val resultChecker = new ResultChecker(spark)
  resultChecker.checkExercise1Result(res3a,"data/exercise1.csv")
  resultChecker.checkExercise2Result(res3b,"data/exercise2.csv")
  resultChecker.checkExercise3Result(res3c,"data/exercise3.json")
  resultChecker.checkExercise4Result(res3d,"data/exercise4.json")

}
