package com.contoso.sandbox.spark

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object CountJPGs {
   def doWork(args: Array[String]) : Unit = {

     var files = "/Users/clomeli/dev/data/ondemand/weblogmini.log"
     if (args.length >= 1) {
        files = args(0)
     }


    val conf = new SparkConf().setAppName("myapp").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

     
     println("found file "+files)
     val wc = sc.textFile(files).count()
           
     println("\nline count = "+wc)
     
     sc.stop()


     //TODO: complete exercise
     println("oh yes you  implemented")
     System.exit(1)

   }
 }

