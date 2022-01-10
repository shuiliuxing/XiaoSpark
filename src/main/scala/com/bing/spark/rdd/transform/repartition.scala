package com.bing.spark.rdd.transform

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object repartition {
  def main(args: Array[String]): Unit = {
    val sp=SparkSession.builder().appName("filterTest").master("local[3]").getOrCreate()
    val sc=sp.sparkContext

    repartition1(sc)
  }

  //简单1
  def repartition1(sc:SparkContext):Unit={
    val rdd=sc.parallelize(Array(1,1,2,2,3,3),3)
    val rdd1=rdd.distinct().collect()
    rdd1.foreach(println)
  }
}
