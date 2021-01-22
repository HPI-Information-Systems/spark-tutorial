package de.hpi.dbsII_exercises

import org.apache.spark.sql.{Dataset, SparkSession}

/***
 * Geben Sie die IDs aller Tabellen zurück (Jede ID nur einmal). Die IDs sollen alpha-numerischsortiert sein
 * @param spark - the spark context
 * @param changeRecords - the input spark dataset containing the change records
 */
class Exercise_3a(spark: SparkSession, changeRecords: Dataset[ChangeRecord]){

  import spark.implicits._

  /***
   *
   * @return sorted sequence of all table ids
   */
  def execute():Seq[String] = {
    ???
  }
}
