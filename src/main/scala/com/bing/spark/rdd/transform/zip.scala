package com.bing.spark.rdd.transform

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object zip {
  def main(args: Array[String]): Unit = {
    val sp=SparkSession.builder().appName("zipTest").master("local").getOrCreate()
    val sc=sp.sparkContext

    zip1(sc)
  }

  def zip1(sc:SparkContext):Unit={
    val a = sc. parallelize (Array(3, 2, 1, 4, 5) , 2)
    val b = sc.parallelize(List("wyp", "blog", "com", "hello", "test"), 2)
    val c = a.zip(b)
    c.foreach(println)
  }
}
