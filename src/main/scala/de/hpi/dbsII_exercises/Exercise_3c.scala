package de.hpi.dbsII_exercises

import java.sql.Timestamp

import org.apache.spark.sql.{Dataset, SparkSession}

class Exercise_3c(spark: SparkSession, changeRecords: Dataset[ChangeRecord]) {

  import spark.implicits._

  //Ausgabenformat: (tableID,attribute,entity,timestamp) -> [Werte zu diesem Zeitpunkt]
  def execute():Map[(String,String,Int,Timestamp),Seq[String]] = {
    ???
  }

}