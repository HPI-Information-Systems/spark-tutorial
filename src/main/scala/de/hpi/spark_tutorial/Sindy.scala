package de.hpi.spark_tutorial

import org.apache.spark.sql.SparkSession

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    import org.apache.spark.sql.sources.DataSourceRegister
    import spark.implicits._

    val tableSetList = inputs.map(f => spark.read
      .option("inferSchema","true")
      .option("header", "true")
      .option("delimiter", ";")
      .csv(f)
      .flatMap(row => row.schema.fields.zipWithIndex.map(tuple => (row.get(tuple._2).toString,tuple._1.name))))

    val unionTables = tableSetList.reduce { (table1, table2) => table1.union(table2) }
      .dropDuplicates

    val groupedValues = unionTables.groupByKey(_._1)

    val keysToSet = groupedValues.mapGroups { case (_, rows) => rows.map(_._2).toSet }
      .dropDuplicates

    val inclusion = keysToSet.flatMap(set => set.map(key => (key, set - key))).show()

/*    val intersect = inclusion.groupByKey(_._1)
      .mapGroups { case (key, iterator) => (key, iterator.foldLeft(iterator.next()._2) { (elem1, elem2) => elem1.intersect(elem2._2) }) }
      .filter(_._2.nonEmpty)
      .sort("_1")


      intersect
      .collect()
      .foreach { case (dependentKey, referencedKey) => println(dependentKey + " < " + referencedKey.toList.sorted.reduce(_ + "," + _)) }*/
  }
}
