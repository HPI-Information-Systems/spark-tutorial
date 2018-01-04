package de.hpi.spark_tutorial

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

object SimpleSpark extends App {

  override def main(args: Array[String]): Unit = {

    var sparkBuilder = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[4]") //local, with 4 cores working

    val spark = sparkBuilder.getOrCreate()

    //importing implict encoders for standard library classes and tuples
    import spark.implicits._

    //-------------------------------------------------------------------------------------------------------------------
    //loading data
    //-------------------------------------------------------------------------------------------------------------------

    //create Dataset programatically:
    val numbers = spark.sqlContext.createDataset((0 until 100).toList)
    //Reading from files:
    val employees = spark.read // reader has specific options
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("src/main/resources/employees.csv").as[(String,Int,Double,String)] //many file formats supported
    //Reading from database: (needs database being setup and accessible as well as driver class in maven)
    val top_templates = spark.sqlContext.read.format("jdbc").
      option("url", "jdbc:postgresql://localhost/changedb").
      option("driver", "org.postgresql.Driver").
      option("useUnicode", "true").
      option("useSSL", "false").
      option("user", "dummy").
      option("password", "dummy").
      option("dbtable","templates_infoboxes").
      load()

    //-------------------------------------------------------------------------------------------------------------------
    //finished loading data
    //-------------------------------------------------------------------------------------------------------------------

    //-------------------------------------------------------------------------------------------------------------------
    //basic transformations
    //-------------------------------------------------------------------------------------------------------------------

    //basic transformations on datasets return new datasets:
    val mapped = numbers.map(i => "This is a number: " + i) //simple map function
    println(mapped.head(5))
    val filtered = mapped.filter(s => s.contains("1")) //simple filter function
    println(filtered.head(5))
    //basic terminal operations:
    val collected = filtered.collect() //collect is a terminal operation that returns the entire dataset to the driver
    val asCsvString = filtered.reduce((s1,s2) => s1 + "," + s2) //reduce combines the values to one value of the same type
    filtered.foreach( s => println(s)) //for each iterates over everything --> why is this almost always a bad idea? What are the use cases?

    // DataFrame and Dataset
    val untyped = numbers.toDF() //data frame is the untyped variant
    val mappedBecomesTyped = untyped.map(r => r.get(0).toString) //map returns a typed dataset again
    val asInteger = untyped.as[Int] //as-function casts to a typed dataset

    //mapping to tuples works
    numbers.map(i => (i,"this is an arbitrary String"))
      .take(10).foreach(println(_))

    //sql on dataframes
    employees.createOrReplaceTempView("employee") //making this dataframe visible as a table
    val sqlResult = spark.sql("SELECT * FROM employee WHERE Age > 95") //performing an sql query on the table
    sqlResult.collect().foreach(println(_))

    //grouping datasets and mapping groups
    val topEarners = employees.groupByKey{case (name,age,salary,company) => company}
      .mapGroups{case (key,iterator) => {
        val topSalary = iterator.toList.maxBy( t => t._3) //what could be problematic here?
        (key,topSalary._1)
      }}
    topEarners.collect().foreach(println(_))

    //-------------------------------------------------------------------------------------------------------------------
    //finished basic transformations
    //-------------------------------------------------------------------------------------------------------------------

    //-------------------------------------------------------------------------------------------------------------------
    //printing info
    //-------------------------------------------------------------------------------------------------------------------

    employees.printSchema()
    topEarners.explain()

    //-------------------------------------------------------------------------------------------------------------------
    //finished printing info
    //-------------------------------------------------------------------------------------------------------------------



    //-------------------------------------------------------------------------------------------------------------------
    //Custom Types
    //-------------------------------------------------------------------------------------------------------------------

    //custom types work out of the box if they are case classes (if implicits are imported:
    val myPersons = spark.createDataset(List(
      MyPerson("Barack","Obama",40),
      MyPerson("George","R.R. Martin",65),
      MyPerson("Elon","Musk",34)))
    myPersons.map(p => p.name + " says hello")
      .collect().foreach(println(_))

    //custom types that are no case classes need to have an encoder defined:
    implicit def changeRecordTupleListEncoder: Encoder[Pet] = org.apache.spark.sql.Encoders.kryo[Pet]

    val pets = spark.createDataset(List(new Pet("Garfield",5),new Pet("Paddington",2)))
    pets.map(p => p.name + " is cute") //encoder gets passed to method implicitly
      .collect().foreach(println(_))

    //-------------------------------------------------------------------------------------------------------------------
    //Finished Custom Types
    //-------------------------------------------------------------------------------------------------------------------



    //TODO: Broadcast variables and accumulators
    val seq = spark.sparkContext.broadcast((1000 until 132435).toSeq)



    // Query: "All persons that share the same gene prefix"
  }

}
