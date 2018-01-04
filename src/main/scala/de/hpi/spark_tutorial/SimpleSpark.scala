package de.hpi.spark_tutorial

import org.apache.spark.sql.{DataFrame, SparkSession}

object SimpleSpark extends App {

  override def main(args: Array[String]): Unit = {
    var sparkBuilder = SparkSession
      .builder()
      .appName("SimpleSpark")
      .master("local[4]")

    val sparkSession = sparkBuilder.getOrCreate()

    import sparkSession.implicits._

    val ds = sparkSession.sqlContext.createDataset((0 until 100).toList)
    ds.map(i => "This is a number: " + i)
      .collect()
      .foreach(println(_))

    // DataFrame to Dataset
    var df : DataFrame = null

    // Dataset to DataFrame


    // Read csv / hdfs


    // Read database


    // Broadcast variables and accumulators
    val seq = sparkSession.sparkContext.broadcast((1000 until 132435).toSeq)



    // Query: "All persons that share the same gene prefix"
  }

}
