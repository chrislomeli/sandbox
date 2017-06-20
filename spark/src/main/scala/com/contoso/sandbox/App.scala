package com.contoso.sandbox

/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println( "Hello World!" )

    val run = CountJPG

    //CountJPG.doWork(args)

    //StreamingLogs.streamLogs(args)

    //PairExercise.runPairExample

    //SparkETL.readActivations

    //KMeans.calculateKMeans

    //StreamingLogsMB.streamLogs()

    //DataSetCSV.csvDataset

    DataSetCSV.csvDataset2

    println( "Googbye World!" )
  }

}
