package com.contoso.sandbox

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions


/**
  * Created by clomeli on 6/16/17.
  */
object PairExercise {

  def runPairExample : Unit = {
    org.apache.log4j.Logger.getRootLogger.setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Application")
      .getOrCreate()

    val sc =  spark.sparkContext
    sc.setLogLevel("ERROR")

    val weblogUserCounts = sc.textFile("/Users/clomeli/dev/training/data/weblogmini.log")
      .map( x => (x.split(' ')(2), 1) )
      .reduceByKey( (x,y)  => x+y )

    val accts = sc.textFile("/Users/clomeli/dev/training/data/accountsmini.txt")
      .map( x =>  x.split(','))
      .map( x => ( x(0) , x) )


    val myjoin = accts.join(weblogUserCounts)
    println("\n===========User counts")
    weblogUserCounts.foreach(println)
    println("\n===========J")
    myjoin.foreach(println)

    spark.stop()
    return



  }

}

