package de.hpi.spark_tutorial

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}

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
//    val top_templates = spark.sqlContext.read.format("jdbc").
//      option("url", "jdbc:postgresql://localhost/changedb").
//      option("driver", "org.postgresql.Driver").
//      option("useUnicode", "true").
//      option("useSSL", "false").
//      option("user", "dummy").
//      option("password", "dummy").
//      option("dbtable","templates_infoboxes").
//      load()

    //-------------------------------------------------------------------------------------------------------------------
    //finished loading data
    //-------------------------------------------------------------------------------------------------------------------

    //-------------------------------------------------------------------------------------------------------------------
    //lamda basics
    //-------------------------------------------------------------------------------------------------------------------

    //spark uses user defined functions to transform data, lets first look at how functions are defined in scala:
    val smallListOfNumbers = List(1,2,3,4,5)
    //map function from int to double:
    def mySimpleMapFunction(i:Int):Double = {
      i*2 +0.5
    }
    //functions can be defined in line (without curly brackets)
    def mySimpleMapFunction2(i:Int):Double = i*2 +0.5
    //scala can infer the return type:
    def mySimpleMapFunction3(i:Int) = i*2 +0.5
    //functions can be anonymous and assigned to variables:
    val myMapFunctionAsFunctionInstance = (i:Int) => i*2 +0.5
    //all variants are the same:
    println(smallListOfNumbers.map(mySimpleMapFunction).head)
    println(smallListOfNumbers.map(myMapFunctionAsFunctionInstance).head)
    println(smallListOfNumbers.map( i => i*2+0.5).head) //anonymous functions can be defined when needed. This allows to also skip the parameter type declaration, since the compiler can infer it
    //more syntactic sugar, if you only use the parameter once you do not need to declare it, simply use it with '_'.
    //This also works for multiple parameters if every parameter is used exactly once (first _ is the first parameter, second underscore the second parameter,...)
    println(smallListOfNumbers.map( _*2+0.5).head)

    //-------------------------------------------------------------------------------------------------------------------
    //finished lamda basics
    //-------------------------------------------------------------------------------------------------------------------


    //-------------------------------------------------------------------------------------------------------------------
    //basic transformations
    //-------------------------------------------------------------------------------------------------------------------

    //basic transformations on datasets return new datasets:
    val mapped = numbers.map(i => "This is a number: " + i) //simple map function, just as seen above with the scala list
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

    //-------------------------------------------------------------------------------------------------------------------
    //Shared Variables across Executors
    //-------------------------------------------------------------------------------------------------------------------
    //the Problem:
    val allowedNames = Set("Berget","Bianka","Cally")
    val ok = employees.filter( e => allowedNames.contains(e._1)) //a copy of allowedNames is shipped to each executor
    val recheck = employees.filter( e => !allowedNames.contains(e._1) && e._2 >60 ) //a copy of allowedNames is shipped to each executor again!
    val forbidden = employees.filter( e => !allowedNames.contains(e._1) && e._2 <= 60) //a copy of allowedNames is shipped to each executor again!
    //if allowedNames is a large data structure this is expensive
    //solution: broadcast variable:
    val allowedNamesBroadcasted = spark.sparkContext.broadcast(allowedNames)
    val okWithBroadcast = employees.filter(e => allowedNamesBroadcasted.value.contains(e._1)) //copy of variable gets shipped and cached to each executor
    val recheckWithBroadcast = employees.filter( e => !allowedNamesBroadcasted.value.contains(e._1) && e._2 >60 ) //allowedNamesBroadcasted is already there, nothing needs to get shipped!
    val forbiddenWithBroadcast = employees.filter( e => !allowedNamesBroadcasted.value.contains(e._1) && e._2 <= 60) //allowedNamesBroadcasted is already there, nothing needs to get shipped!
    //destroy the variable to release memory:
    allowedNamesBroadcasted.destroy()

    //accumulators: in principle these are shared variables that you can add to. This gives you the opportunity to add a side-effect to a computation.
    //Only use them with an Action, not a transformation, transformations should always be free of side effects as they may be rerun multiple times!
    //in practice, use with care: http://imranrashid.com/posts/Spark-Accumulators/
    val appleAccumulator = spark.sparkContext.longAccumulator("My Accumulator")
    recheck.foreach( e => if(e._4 == "Apple") appleAccumulator.add(1)) //really only useful if you do something else in the foreach and want to add to the accumulator while you are doing it (for example for debug info) - otherwise it is simpler to just use filter and count!
    println("There are " + appleAccumulator.value + " employees of apple in the recheck dataset")

    //-------------------------------------------------------------------------------------------------------------------
    //Finished Variables across Executors
    //-------------------------------------------------------------------------------------------------------------------


    //-------------------------------------------------------------------------------------------------------------------
    //Basic Machine Learning example:
    //This is a slightly simplified and more commented version of https://spark.apache.org/docs/2.2.0/ml-classification-regression.html#decision-tree-classifier
    //-------------------------------------------------------------------------------------------------------------------
    val data = spark.read.format("libsvm").load("src/main/resources/sample_libsvm_data.txt")

    data.printSchema() //libsvm data format is read into spark into a data frame with two columns: a label and all the features

    //new concepts to understand about apache spark machine learning:
    /*
    Transformer:   A Transformer is an algorithm which can transform one DataFrame into another DataFrame. E.g., an ML model is a Transformer which transforms a DataFrame with features into a DataFrame with predictions.
    Estimator:     An Estimator is an algorithm which can be fit on a DataFrame to produce a Transformer. E.g., a learning algorithm is an Estimator which trains on a DataFrame and produces a model. (Example: DecisionTree)
    Pipeline:      A Pipeline chains multiple Transformers and Estimators together to specify an ML workflow.
     */

    // Automatically identify categorical features, and index them. The Vectors only contain numerical values, so we need to flag which values are categorical
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
      .fit(data)
    //A VectorIndexer is a transformer which means that it transforms a data frame (in this case, a column gets added)

    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    // Create a Decision Tree Model:
    val dt = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("indexedFeatures")

    // Chain indexer and tree in a Pipeline:
    val pipeline = new Pipeline()
      .setStages(Array( featureIndexer, dt))

    // Run the entire pipeline - this first runs the indexer - which adds the column indexedFeatures, then trains the decision tree model
    val model = pipeline.fit(trainingData) //the model is a new transformer

    // Make predictions - these are now added to the dataframe as a new column (default name: "prediction")
    val predictions = model.transform(testData)

    // Select example rows to display.
    predictions.select("prediction", "label", "features","indexedFeatures").show(5)

    // Select (prediction, true label) and compute test error. Convenience class for calculating test errir
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))

    //print the tree model:
    val treeModel = model.stages(1).asInstanceOf[DecisionTreeClassificationModel]
    println("Learned classification tree model:\n" + treeModel.toDebugString)

  }

}
