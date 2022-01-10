package com.bing.spark.rdd.transform

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object intersection {
  def main(args: Array[String]): Unit = {
    val sp=SparkSession.builder().appName("unionTest").master("local").getOrCreate()
    val sc=sp.sparkContext

    intersection1(sc)
  }

  def intersection1(sc:SparkContext):Unit={
    val rdd1=sc.parallelize(List(1,2,3,4))
    val rdd2=sc.parallelize(List(1,2))
    val rdd=rdd1.intersection(rdd2)
    rdd.foreach(println)
  }
}
