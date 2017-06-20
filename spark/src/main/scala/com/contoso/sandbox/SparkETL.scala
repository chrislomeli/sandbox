package com.contoso.sandbox
import org.apache.spark.{SparkConf, SparkContext}

import scala.xml._

/**
  * Created by clomeli on 6/16/17.
  */
object SparkETL {

  def getActivations(xmlstring: String): Iterator[Node] = {
    val nodes = XML.loadString(xmlstring) \\ "activation"
    nodes.toIterator
  }

  // Given an activation record (XML Node), return the model name
  def getModel(activation: Node): String = {
    (activation \ "model").text
  }

  // Given an activation record (XML Node), return the account number
  def getAccount(activation: Node): String = {
    (activation \ "account-number").text
  }


  def readActivations : Unit = {

    val files="""/Users/clomeli/dev/training/data/activations/2008-10.xml"""
    val conf = new SparkConf().setAppName("myapp").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val activationFiles = sc.wholeTextFiles(files)

    // Parse each file (as a string) into a collection of activation XML records
    val activationTrees = activationFiles.flatMap(pair => getActivations(pair._2))

    // Map each activation record to (account-number, model name)
    val models = activationTrees.map(record => getAccount(record) + ":" + getModel(record))

    models.foreach(println)


    // Save the data to a file
    //models.saveAsTextFile("/Users/clomeli/dev/training/data/activations_out.txt")

  }


}
