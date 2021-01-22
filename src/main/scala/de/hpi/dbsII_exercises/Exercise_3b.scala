package de.hpi.dbsII_exercises

import org.apache.spark.sql.{Dataset, SparkSession}

/***
 * Geben Sie für jede Tabelle die Anzahl der Attribute zurück.
 * @param spark - the spark context
 * @param changeRecords - the input spark dataset containing the change records
 */
class Exercise_3b(spark: SparkSession, changeRecords: Dataset[ChangeRecord]) {

  import spark.implicits._

  /***
   *
   * @return A Map that maps a table-id (key) to the number of attributes of the corresponding table (value)
   */
  def execute():Map[String,Int] = {
    ???
  }

}
