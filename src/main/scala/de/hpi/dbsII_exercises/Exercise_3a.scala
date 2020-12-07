package de.hpi.dbsII_exercises

import org.apache.spark.sql.{Dataset, SparkSession}


class Exercise_3a(spark: SparkSession, changeRecords: Dataset[ChangeRecord]){

  import spark.implicits._

  def execute():Seq[String] = {
    ???
  }
}
