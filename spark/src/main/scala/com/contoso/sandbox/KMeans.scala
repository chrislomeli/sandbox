package com.contoso.sandbox

import org.apache.spark.{SparkConf, SparkContext}

import scala.math.pow


/**
  * Created by clomeli on 6/16/17.
  */
object KMeans {

  // Find K Means of Loudacre device status locations
  //
  // Input data: file(s) with device status data (delimited by ',')
  // including latitude (4th field) and longitude (5th field) of device locations
  // (lat,lon of 0,0 indicates unknown location)

  // The squared distances between two points
  def distanceSquared(p1: (Double,Double), p2: (Double,Double)) = {
    pow(p1._1 - p2._1,2) + pow(p1._2 - p2._2,2 )
  }

  // The sum of two points
  def addPoints(p1: (Double,Double), p2: (Double,Double)) = {
    (p1._1 + p2._1, p1._2 + p2._2)
  }

  // for a point p and an array of points, return the index in the array of the point closest to p
  def closestPoint(p: (Double,Double), points: Array[(Double,Double)]): Int = {
    var index = 0
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- 0 until points.length) {
      val dist = distanceSquared(p,points(i))
//      println("Compare (%f,%f) to (%f, %f) distinace squared : %f".format(p._1, p._2, points(i)._1, points(i)._2, dist))
      if (dist < closest) {
        closest = dist
        bestIndex = i
      }
    }
    bestIndex
  }


  def calculateKMeans : Unit = {

    // The device status data file(s)
    val filename = "/Users/clomeli/dev/training/data/devicestatus_etl.txt"
    val conf = new SparkConf().setAppName("myapp").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // K is the number of means (center points of clusters) to find
    val K = 5

    // ConvergeDist -- the threshold "distance" between iterations at which we decide we are done
    val convergeDist = .1

    // Parse the device status data file
    // Split by delimiter ,
    // Parse  latitude and longitude (4th and 5th fields) into pairs
    // Filter out records where lat/long is unavailable -- ie: 0/0 points
    println("Get %s ".format(filename))
    val points = sc.textFile(filename).
      map(line => line.split(',')).
      map(fields => (fields(3).toDouble, fields(4).toDouble)).
      filter(point => !((point._1 == 0) && (point._2 == 0)))
      .persist()

    //start with K randomly selected points from the dataset
    val kPoints = points.takeSample(false, K, 34)
    println("\n===========\nStarting K points:\n===========")
    kPoints.foreach(println)

    // loop until the total distance between one iteration's points and the next is less than the convergence distance specified
    var tempDist = Double.PositiveInfinity
    while (tempDist > convergeDist) {

      // for each point, find the index of the closest kpoint.  map to (index, (point,1))
      val closest = points.map(p => (closestPoint(p, kPoints), (p, 1)))
      //println("\n===========\nSample of closest:\n===========")
      //for (i <- closest ) {
      //  println(i)
      //}


      // For each key (k-point index), reduce by adding the coordinates and number of points
      val pointStats = closest.reduceByKey { case ((point1, n1), (point2, n2)) => (addPoints(point1, point2), n1 + n2) }
      //println("\n===========\nSample of closest Reduced:\n===========")
      //pointStats.take(20).foreach(println)

      // For each key (k-point index), find a new point by calculating the average of each closest point
      val newPoints = pointStats.map { case (i, (point, n)) => (i, (point._1 / n, point._2 / n)) }.collectAsMap()
      //println("\n===========\nNew point:\n===========")
      //newPoints.take(20).foreach(println)

      // calculate the total of the distance between the current points and new points
      tempDist = 0.0
      for (i <- 0 until K) {
        tempDist += distanceSquared(kPoints(i), newPoints(i))
      }
      println("Distance between iterations: " + tempDist)

      // Copy the new points to the kPoints array for the next iteration
      for (i <- 0 until K) {
        kPoints(i) = newPoints(i)
      }
    }

    // Display the final center points
    println("Final K points: ")
    kPoints.foreach(println)
  }
}
