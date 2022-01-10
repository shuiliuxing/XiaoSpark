package com.bing.spark.rdd.transform

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object map {
  def main(args:Array[String]):Unit={
    val sp=SparkSession.builder().appName("mapTest").master("local[3]").getOrCreate()
    val sc=sp.sparkContext

    mapTestThree(sc)

  }

    //简单1
  def mapTestOne(sc:SparkContext): Unit ={
    val rdd=sc.parallelize(List(1,2,3,3))
    val addRdd=rdd.map(line=>line+1)
    addRdd.foreach(println)
  }

  //简单2
  def mapTestTwo(sc:SparkContext):Unit={
    val rdd=sc.parallelize(List(
                                  ("spark",10),
                                  ("hadoop",4),
                                  ("spark",20)
                               )
                          )
    val testRdd=rdd.map(line=>(line,1))
    testRdd.collect.foreach(println)
  }

  //简单3
  def mapTestThree(sc:SparkContext):Unit={
    val rdd=sc.textFile("D:\\spark\\rdd\\transformation\\map\\one.txt")
    val testRdd=rdd.map(line=>line.split(" ")).collect()
    for (i <- 0 to 3){
      for(j <- 0 to 1){
        print(testRdd(i)(j)+"\t")
      }
      println()
    }
  }
}
