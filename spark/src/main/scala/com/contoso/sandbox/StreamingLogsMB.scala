package com.contoso.sandbox

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by clomeli on 6/16/17.
  */
object StreamingLogsMB {

  // Given an array of new counts, add up the counts
  // and then add them to the old counts and return the new total
  def updateCount = (newCounts: Seq[Int], state: Option[Int]) => {
    val newCount = newCounts.foldLeft(0)(_ + _)
    val previousCount = state.getOrElse(0)
    Some(newCount + previousCount)
  }


  def streamLogs() {

    // get hostname and port of data source from application arguments
    val hostname = "localhost"
    val port = 9988

    // Create a Spark Context
    val conf = new SparkConf().setAppName(StreamingLogs.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // Configure the Streaming Context with a 1 second batch duration
    val ssc = new StreamingContext(sc,Seconds(1))

    // Create a DStream of log data from the server and port specified
    val logs = ssc.socketTextStream(hostname,port)

    // Every two seconds, display the total number of requests over the
    // last 5 seconds
    logs.countByWindow(Seconds(5),Seconds(2)).print()

    // Bonus: Display the top 5 users every second

    // Enable checkpointing (required for all window and state operations)
    ssc.checkpoint("logcheckpt")

    // Count requests by user ID for every batch
    val userreqs = logs.
      map(line => (line.split(" ")(2),1)).
      reduceByKey((x,y) => x+y)

    // Update total user requests
    val totalUserreqs = userreqs.updateStateByKey(updateCount)

    // Sort each state RDD by hit count in descending order
    val topUserreqs=totalUserreqs.
      map(pair => pair.swap).
      transform(rdd => rdd.sortByKey(false)).
      map(pair => pair.swap)

    topUserreqs.print()



    ssc.start()
    ssc.awaitTermination()
  }

}
