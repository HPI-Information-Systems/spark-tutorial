package de.hpi.dbsII_exercises

import java.sql.Timestamp

import org.apache.spark.sql.{Dataset, SparkSession}

class Exercise_3d(spark: SparkSession, changeRecords: Dataset[ChangeRecord]) {

  import spark.implicits._

  def execute():Map[Seq[Timestamp],Seq[(String,String)]] = {
    ???
  }

}
