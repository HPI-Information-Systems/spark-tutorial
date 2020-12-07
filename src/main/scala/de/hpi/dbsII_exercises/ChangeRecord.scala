package de.hpi.dbsII_exercises

import java.sql.Timestamp

import org.apache.spark.sql.Row

case class ChangeRecord(tableID:String, timestamp:Timestamp, entityID:Int, attributeName:String, newValue:String)

object ChangeRecord {
  def from(r: Row): ChangeRecord = {
    ChangeRecord(r.getString(0),
      r.getTimestamp(1),
      r.getInt(2),
      r.getString(3),
      r.getString(4)
    )
  }
}

