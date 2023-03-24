package de.hpi.dbsII_exercises

case class ColumnVersion(revisionID: String,
                         revisionDate: String,
                         values: Set[String],
                         header:Option[String],
                         position:Option[Int],
                         columnNotPresent:Boolean) {

}
