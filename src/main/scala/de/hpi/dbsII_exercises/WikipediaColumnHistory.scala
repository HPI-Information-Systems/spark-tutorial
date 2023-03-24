package de.hpi.dbsII_exercises

case class WikipediaColumnHistory(id: String,
                         tableId: String,
                         pageID: String,
                         pageTitle: String,
                         columnVersions: IndexedSeq[ColumnVersion])
