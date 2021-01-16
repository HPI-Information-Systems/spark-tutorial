package de.hpi.getting_started

object ScalaIntroduction extends App {

  //Immutable and mutable Variables
  val a:String = "hello" //immutable variable
  var b:Int = 3 //mutable variable
  b = 4
  //type inference by the compiler
  val c = 1
  val d = 1.5
  val e = c + d // still works
  val list = List(1,2,3)
  val list2 = List(1,2,3.4)
  val list3 = List(1,2,"a") //defaults to Any (similar to java Object)

  //functions/methods
  def addTwo(i:Int) = {
    val result = i+2
    result //no return statement necessary: the result of the last expression will be returned
  }
  def printHello = { // functions/method without parameters don't need parentheses
    println("Hello World")
  }
  printHello
  //printHello() - trying to call them with parentheses if they are defined without actually does not compile
  //CONVENTION: If a parameterless method has side-effects (such as printing or updating states) it is defined with parentheses
  //if it just returns stored or computed values (such as the size of a list), it is defined without parentheses
  val f = addTwo(c)
  //Scala encourages functional programming: functions are objects and can be stored in variables:
  val myFunction:(Int => Int) = addTwo
  println(myFunction(3) == addTwo(3))
  //we can define anonymous functions:
  val myStringReverseFunction = (string:String) => string.reverse
  println(myStringReverseFunction(a))

  //access to java standard library:
  val file = new java.io.File("path")

  //collections - immutable
  val myList = Seq(1,2,3)
  //myList(0) = 1 - does not work
  //the only way to change immutable collections is through transformations (which create new collections):
  val withOutFIrstElement = myList.tail
  val longerList = myList ++ Seq(2,3,4,5) //concatenation
  //Many functions on collections expect other functions that they apply to the collection. We can use them to create transformation pipelines
  val longerListTransformed = longerList
    .map(addTwo) //map function expects another function that is applied to every element
    .filter(n => n != 3) //filter keeps all elements that fulfill the predicate.
    .sorted //self-explanatory
  //More syntactic sugar: If an anonymous function uses every parameter exactly once, we can skip the parameter declaration and use _ instead:
  val productVariant1 = longerListTransformed
    .reduce((a,b) => a*b) //reduce expects a function that merges two elements. It applies this function on the first pair of values and plugs the result r into the next function call together with the third element, and so on....
  val productVariant2 = longerListTransformed
    .reduce(_*_)
  println(productVariant1)
  println(productVariant2)

  //there are also mutable collections:
  val mutableSequence = scala.collection.mutable.Seq(1,2,3)
  mutableSequence(0) = 4 //this is allowed now
  println(mutableSequence)
  //we still don't have an add/append function for Seq or IndexedSeq - those are fixed-length lists
  //for variable-length lists we can use the following:
  val arrayList = scala.collection.mutable.ArrayBuffer(1,2,3)
  arrayList.append(4)
  val linkedList = scala.collection.mutable.ListBuffer(1,2,3)
  linkedList.append(4)
  println(arrayList)
  println(linkedList)
  //transformation-functions still return a NEW collection:
  val transformedArrayList = arrayList
    .map(i => i*2)
  println(transformedArrayList)
  println(arrayList)

  //we have a lot of convenience functions to transform collections into each other
  val asSet = arrayList.toSet
  val asMap = arrayList
    .groupBy(i => i%2 == 0)

  //standard loops:
  for( i <- 0 until arrayList.size){
    println(arrayList(i))
  }
  //The scala-way: standard loop as function on the list:
  arrayList.foreach(elem => {
    println(elem)
  })
  //while-loops also exist - but it is rare to use them
  var i=0
  while(i<arrayList.size){
    println(arrayList(i))
    i+=1
  }

  //scala has built-In tuple classes to group variables that belong together:
  val myTuple = ("first","second",3)
  println(myTuple._1)
  println(myTuple._2)
  println(myTuple._3)
  //these interact nicely with maps
  val myMap = Map(("firstKey",1),("secondKey",2),("thirdKey",3))
  myMap.foreach(t => { //t is a tuple of key and value
    println("key:" + t._1)
    println("value:" + t._2)
    //we can also assign tuple contents to named variables:
    val (key,value) = t
    println(key)
    println(value)
  })

}
