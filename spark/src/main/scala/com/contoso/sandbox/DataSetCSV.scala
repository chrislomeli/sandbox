package com.contoso.sandbox

import java.sql.Timestamp

import org.apache.log4j.Level
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


/**
  * Created by clomeli on 6/16/17.
  */
object DataSetCSV {

  case class Account (acctId:Long, startTime:Timestamp, endTimeStr:String, first:String, last:String, address:String, city:String, state:String, zip:String, longID:Long, createdTime:Timestamp, modifiedTime:String)



  // Define a UDF that wraps the upper Scala function defined above
  // You could also define the function in place, i.e. inside udf
  // but separating Scala functions from Spark SQL's UDFs allows for easier testing

  def csvDataset : Unit = {
    org.apache.log4j.Logger.getRootLogger.setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Application")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val customSchema = StructType(Array(
      StructField("acctId", LongType, true),
      StructField("startTime", TimestampType, true),
      StructField("endTimeStr", StringType, true),
      StructField("firstName", StringType, true),
      StructField("lastName", StringType, true),
      StructField("address", StringType, true),
      StructField("city", StringType, true),
      StructField("state", StringType, true),
      StructField("zipCode", StringType, true),
      StructField("longId", LongType, true),
      StructField("createdTime", TimestampType, true),
      StructField("modifiedTime", TimestampType, true)
    ))

    val ds = spark.read.schema(customSchema).csv("/Users/clomeli/dev/training/data/accounts.txt")
        .withColumn("endTime", col("endTimeStr").cast("timestamp"))
        .withColumnRenamed("zipCode", "zip")

    ds.printSchema()
    ds.show(10)

    spark.stop()
    return

  }

  /**
    *
    */

  def csvDataset2 : Unit = {
    org.apache.log4j.Logger.getRootLogger.setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Application")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val personEncoder = Encoders.product[Account]

    val customSchema = personEncoder.schema

/*
     val customSchema = StructType(Array(
      StructField("acctId", LongType, true),
      StructField("startTime", TimestampType, true),
      StructField("endTimeStr", StringType, true),
      StructField("firstName", StringType, true),
      StructField("lastName", StringType, true),
      StructField("address", StringType, true),
      StructField("city", StringType, true),
      StructField("state", StringType, true),
      StructField("zipCode", StringType, true),
      StructField("longId", LongType, true),
      StructField("createdTime", TimestampType, true),
      StructField("modifiedTime", TimestampType, true)
    ))
*/
    val ds = spark.read.schema(customSchema).csv("/Users/clomeli/dev/training/data/accounts.txt")
      .withColumn("endTime", col("endTimeStr").cast("timestamp"))
      .withColumnRenamed("zipCode", "zip")

    ds.printSchema()
    ds.show(10)

    spark.stop()
    return

  }

}

