package core
import org.apache.spark.{SparkConf, SparkContext}
object SparkAssignment45 {

  def main(args: Array[String]): Unit = {
    // spark Assignment45 Task1
    val myList = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    println("list is "+myList)
    // print sum of the list elements
    var sum = 0
    var count = 0
    for(x <- myList) {
      sum += x
      count += 1
    }
    println("sum of the list elements: "+sum)
    // print count of the list elements
    println("number of elements: "+count)
    //print average
    println("average of the list elements: "+ sum/count)
    // print the sum of all even numbers
    var evensum = 0
    for(x <- myList){
      if(x%2 == 0){
        evensum += x
      }
    }
    println("sum of the even elements: "+evensum)

    // count of the elements divisible by 5 and 3
    var divcount = 0
    for(x <- myList){
      if(x%3 == 0 && x%5 == 0){
        divcount += 1
      }
    }
    println("count of the elements divisible by 5 and 3: "+divcount)

    //spark Assignment 45 task 2
    /*
    ---limitations of MapReduce
    -> disk based computation so it is slow
    -> not polyglot
    ->
     */
    /*
    ---RDD and its features
    -> Resilient distributed data
    -> RDD is involved with data partition and data distribution behind the scenes
    -> RDD is fault-tolerant: can be re computed in case of node failure
    -> RDD is core data structure of the spark and cannot be changes once it is created
    ->RDD can be transformed using spark Transformations and Actions
    ->
     */
    /*
    ---RDD Operations
    -> Transformations: Creates a new RDD from existing one
       ->Map: applies a function to each element to create a new RDD
       ->Filter: filter elements based on criteria and returns a new RDD
    -> Actions: returns output
       ->Reduce: aggregates the elements
       ->Collect: returns the elements as an array
       ->Count: returns the number of elements
     */
    // spark Assignment 45 task 3
    val sc = new SparkContext("local[*]", "SparkAssignment45")
    val lines = sc.textFile("../session_45_3.txt")
    val linecount = lines.count()
    println("Number of rows in the file: "+linecount)
    //word counts
    //val allwords = lines.map(x => x.toString().split("-"))
    val allwords = lines.flatMap(x => x.split("-"))
    val wordcount = allwords.count()
       println(wordcount)
    val wordCounts = allwords.countByValue()

    // Print the results.
    wordCounts.foreach(println)
    //println("Number of words in the file: "+allwords.count())
  }

}
