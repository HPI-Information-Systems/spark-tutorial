package de.hpi.spark_tutorial

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.log4j.Logger
import org.apache.log4j.Level

// A Scala case class; works out of the box as Dataset type using Spark's implicit encoders
case class Person(name:String, surname:String, age:Int)

object SparkIntroduction extends App {

  override def main(args: Array[String]): Unit = {

    // Turn off logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //------------------------------------------------------------------------------------------------------------------
    // Lambda basics (for Scala)
    //------------------------------------------------------------------------------------------------------------------

    //spark uses user defined functions to transform data, lets first look at how functions are defined in scala:
    val smallListOfNumbers = List(1, 2, 3, 4, 5)

    // A Scala map function from int to double
    def squareAndAdd(i: Int): Double = {
      i * i + 0.5
    }
    // A Scala map function defined in-line (without curly brackets)
    def squareAndAdd2(i: Int): Double = i * i + 0.5
    // A Scala map function inferring the return type
    def squareAndAdd3(i: Int) = i * i + 0.5
    // An anonymous Scala map function assigned to a variable
    val squareAndAddFunction = (i: Int) => i * i + 0.5

    println("---------------------------------------------------------------------------------------------------------")

    // Different variants to apply the same function
    println(smallListOfNumbers.map(squareAndAdd))
    println(smallListOfNumbers.map(squareAndAdd2))
    println(smallListOfNumbers.map(squareAndAdd3))
    println(smallListOfNumbers.map(squareAndAddFunction))
    println(smallListOfNumbers.map(i => i * 2 + 0.5)) // anonymous function; compiler can infers types
    println(smallListOfNumbers.map(_ * 2 + 0.5)) // syntactic sugar: '_' maps to first (second, third, ...) parameter

    //------------------------------------------------------------------------------------------------------------------
    // Setting up a Spark Session
    //------------------------------------------------------------------------------------------------------------------

    // Create a SparkSession to work with Spark
    val sparkBuilder = SparkSession
      .builder()
      .appName("SparkTutorial")
      .master("local[4]") // local, with 4 worker cores
    val spark = sparkBuilder.getOrCreate()

    // Set the default number of shuffle partitions (default is 200, which is too high for local deployment)
    spark.conf.set("spark.sql.shuffle.partitions", "8") //

    // Importing implicit encoders for standard library classes and tuples that are used as Dataset types
    import spark.implicits._

    println("---------------------------------------------------------------------------------------------------------")

    //------------------------------------------------------------------------------------------------------------------
    // Loading data
    //------------------------------------------------------------------------------------------------------------------

    // Create a Dataset programmatically
    val numbers = spark.createDataset((0 until 100).toList)

    println("---------------------------------------------------------------------------------------------------------")

    //------------------------------------------------------------------------------------------------------------------
    // Basic transformations
    //------------------------------------------------------------------------------------------------------------------

    // Basic transformations on datasets return new datasets
    val mapped = numbers.map(i => "This is a number: " + i)
    val filtered = mapped.filter(s => s.contains("1"))
    val sorted = filtered.sort()
    List(numbers, mapped, filtered, sorted).foreach(dataset => println(dataset.show(5)))

    println("---------------------------------------------------------------------------------------------------------")

    // Basic terminal operations
    filtered.foreach(s => println(s)) // performs an action for each element (on the node, where this is being executed, can be anywhere in the cluster!)
    val collected = filtered.collect() // collects the entire dataset to the driver process (if it is too large this will cause an OutOfMemory Error)
    val reduced = filtered.reduce((s1, s2) => s1 + "," + s2) // reduces all values successively to a single one using the function
    List(collected, reduced).foreach(result => println(result.getClass))

    println("---------------------------------------------------------------------------------------------------------")

    // DataFrame and Dataset
    val untypedDF = numbers.toDF() // converts typed dataset to untyped dataframe
    val stringTypedDS = untypedDF.map(r => r.get(0).toString) // map function on a dataframe returns a typed dataset
    val integerTypedDS = untypedDF.as[Int] // casts dataframe to a dataset of a concrete types
    List(untypedDF, stringTypedDS, integerTypedDS).foreach(result => println(result.head.getClass))
    List(untypedDF, stringTypedDS, integerTypedDS).foreach(result => println(result.head))

    println("---------------------------------------------------------------------------------------------------------")

    // Datasets can contain typed tuples (scala tuples) to represent multiple columns
    val multiColumnDataset = numbers
      .map(i => (i, "nonce", 3.1415, true))
    multiColumnDataset
      .take(10) //take copies the contents to the driver process
      .foreach(println(_))

    println("---------------------------------------------------------------------------------------------------------")

    // Read a Dataset from a file
    val employees = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("data/employees.csv") // also text, json, jdbc, parquet
      .as[(String, Int, Double, String)]
    // SQL on DataFrames
    employees.createOrReplaceTempView("employee") // make this dataframe visible as a table
    val sqlResult = spark.sql("SELECT * FROM employee WHERE Age > 95") // perform an sql query on the table

    import org.apache.spark.sql.functions._

    sqlResult // results of sql queries are dataframes
      .as[(String, Int, Double, String)] // DS
      .sort(desc("Salary")) // desc() is a standard function from the spark.sql.functions package
      .head(10)
      .foreach(println(_))

    println("---------------------------------------------------------------------------------------------------------")

    // Grouping and aggregation for typed Datasets:
    val topEarners = employees
      .groupByKey { case (name, age, salary, company) => company }
      .mapGroups { case (key, iterator) =>
          val topEarner = iterator.toList.maxBy(t => t._3) // could be problematic: Why?
          (key, topEarner._1, topEarner._3)
      }
      .sort(desc("_3"))
    topEarners.collect().foreach(t => println(t._1 + "'s top earner is " + t._2 + " with salary " + t._3))

    println("---------------------------------------------------------------------------------------------------------")

    //------------------------------------------------------------------------------------------------------------------
    // Analyzing Datasets and DataFrames
    //------------------------------------------------------------------------------------------------------------------

    employees.printSchema() // print schema of dataset/dataframe
    topEarners.explain() // print Spark's physical query plan for this dataset/dataframe
    topEarners.show() // print the top 20 rows of this dataset/dataframe

    //------------------------------------------------------------------------------------------------------------------
    // Custom types
    //------------------------------------------------------------------------------------------------------------------

    // (see class definition above) A Scala case class works out of the box as Dataset type using Spark's implicit encoders
    //   case class Person(name:String, surname:String, age:Int)

    val persons = spark.createDataset(List(
      Person("Barack", "Obama", 40),
      Person("George", "R.R. Martin", 65),
      Person("Elon", "Musk", 34)))

    persons
      .map(_.name + " says hello")
      .collect()
      .foreach(println(_))

    println("------------------------------------------")

  }
}
