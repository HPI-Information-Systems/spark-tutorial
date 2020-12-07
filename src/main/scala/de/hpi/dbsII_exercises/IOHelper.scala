package de.hpi.dbsII_exercises

import java.sql.Timestamp

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}


object IOHelper {

  def writeSparkJson(spark: SparkSession, ds: Dataset[_], path: String) = {
    ds
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .json(path)
  }

  def writeSparkCSV(spark: SparkSession, ds: Dataset[_], path:String) = {
    ds.coalesce(1)
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .option("header","false")
      .option("delimiter", ",")
      .option("quote", "\"")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.S") //2019-11-01 00:00:00.0
      .save(path)
  }

  def readSparkJson(spark:SparkSession,path: String) = {
    spark.read.json(path)
  }

  def readSparkCSV(spark:SparkSession,pathToData: String,hasHeader:Boolean) = {
    spark.read.format("csv")
      .option("delimiter", ",")
      .option("quote", "\"")
      .option("header", hasHeader)
      .option("inferSchema", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.S") //2019-11-01 00:00:00.0
      .load(pathToData)
  }

}
