package de.hpi.dbsII_exercises

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession

class ResultChecker(spark:SparkSession) {

  import spark.implicits._

  def checkExercise1Result(res1: Seq[String], path: String) = {
    val expected = IOHelper.readSparkCSV(spark,path,false)
      .as[String]
      .collect()
      .sorted
    checkForEquality(1,res1, expected)
  }

  def checkExercise2Result(res2: Map[String, Int], path: String) = {
    val expected = IOHelper.readSparkCSV(spark,path,false)
      .as[(String,Int)]
      .collect()
      .sorted
    checkForEquality(2,res2.toIndexedSeq.sorted,expected)
  }

  def checkExercise3Result(res1:  Map[(String, String, Int, Timestamp), Seq[String]], path: String) = {
    val expected = IOHelper.readSparkJson(spark,path)
      .as[((String, String, BigInt, Timestamp), Seq[String])]
      .map{case ((d,a,e,t),vals) => ((d,a,e.toInt,t),vals)}
      .collect()
      .toList
      .sortBy{case ((ds,attr,e,t),vals) => (ds,attr,e,t.getTime)}
    val resultSorted = res1
      .toList
      .sortBy{case ((ds,attr,e,t),vals) => (ds,attr,e,t.getTime)}
    checkForEquality(3,resultSorted, expected)
  }

  def checkExercise4Result(res4: Map[Seq[Timestamp], Seq[(String, String)]], path: String) = {
    val expected = IOHelper.readSparkJson(spark,path)
      .as[(Seq[Timestamp], Seq[(String, String)])]
      .collect()
      .toList
      .sortBy{case (timestamps,vals) => timestamps}(timestampListOrdering)
    val resultSorted = res4
      .toList
      .sortBy{case (timestamps,vals) => timestamps}(timestampListOrdering)
    if (resultSorted == expected || resultSorted == expected.filter(_._1.size>0)) {
      println(s"Exercise 4 result was correct")
    } else {
      println(s"Exercise 4 result was incorrect")
    }
  }

  private def checkForEquality(exerciseNumber:Int, result: Seq[Any], expected: Seq[Any]) = {
    if (result.toList == expected.toList) {
      println(s"Exercise $exerciseNumber result was correct")
    } else {
      println(s"Exercise $exerciseNumber result was incorrect")
    }
  }

  implicit def timestampListOrdering = new Ordering[Seq[Timestamp]] {

    override def compare(x: Seq[Timestamp], y: Seq[Timestamp]): Int = {
      var res = 0
      val itX = x.iterator
      val itY = y.iterator
      while(itX.hasNext && itY.hasNext && res==0){
        val curX = itX.next().getTime
        val curY = itY.next().getTime
        if(curX>curY)
          res = 1
        else if (curX < curY)
          res = -1
      }
      if(res==0 && itX.hasNext) {
        1
      } else if(res==0 && itY.hasNext){
        -1
      } else {
        res
      }
    }
  }

}
