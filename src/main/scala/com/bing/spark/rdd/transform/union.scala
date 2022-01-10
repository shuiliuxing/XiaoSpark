package com.bing.spark.rdd.transform

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object union {
  def main(args: Array[String]): Unit = {
    val sp=SparkSession.builder().appName("unionTest").master("local").getOrCreate()
    val sc=sp.sparkContext

    union1(sc)
  }

  def union1(sc:SparkContext):Unit={
    val rdd1=sc.parallelize(Array(1,2,3))
    val rdd2=sc.parallelize(Array(4,5,6))
    val rdd=rdd1.union(rdd2)
    rdd.foreach(println)
  }
}
