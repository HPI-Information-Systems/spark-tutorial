package de.hpi.dbsII_exercises

import java.sql.Timestamp

import org.apache.spark.sql.{Dataset, SparkSession}

/***
 * Bestimmen Sie die Change-Signatures aller  Attribute. Die Change Signature ist eine sortierte Liste an Zeitpunkten,
 * zu denen viele Änderungen an dem Attribut stattfanden. Ein Zeitpunkt soll genau dann in der Change Signature eines Attributes A auftauchen,
 * wenn mindestens 100 entitities zu diesem Zeitpunkt im Attribut A ihren Wert geändert haben. Geben Sie für jede Change-Signature alle Attribute
 * (Identifiziert durch die Kombination aus tableID und attributeName) zurück, die diese Change-Signature aufweisen.
 * @param spark - the spark context
 * @param changeRecords - the input spark dataset containing the change records
 */
class Exercise_3d(spark: SparkSession, changeRecords: Dataset[ChangeRecord]) {

  import spark.implicits._
  /***
   *
   * @return a map containing all change signatures that appear in the data. Format:
   *         Key: the change-signature, represented as a sorted sequence of Timestamps
   *         Value: a sequence of all attributes (represented as tuples in the form of (tableID,attrID) ), that belong to this change-signature
   */
  def execute():Map[Seq[Timestamp],Seq[(String,String)]] = {
    ???
  }

}
