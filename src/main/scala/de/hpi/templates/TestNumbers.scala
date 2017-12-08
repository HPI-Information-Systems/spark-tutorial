package de.hpi.templates

import org.apache.spark.sql.SparkSession

/**
 * @author ${user.name}
 */
object TestNumbers extends App {

  override def main(args: Array[String]): Unit = {
    var sparkBuilder = SparkSession
      .builder()
      .appName("Spark SQL basic example")
    if(args.length==1 && args(0) == "local" ){
      sparkBuilder = sparkBuilder.master("local[1]")
    }
    val spark = sparkBuilder.getOrCreate()
    import spark.implicits._

    val df = spark.sqlContext.createDataset((0 until 100).toList)
    df.map(i => "This is a number: " + i)
      .collect().foreach(println(_))
  }

}
