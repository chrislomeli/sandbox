package com.contoso.sandbox.spark

/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println( "Hello World!" )
    CountJPGs.doWork(args)
    println( "Googbye World!" )
  }

}
