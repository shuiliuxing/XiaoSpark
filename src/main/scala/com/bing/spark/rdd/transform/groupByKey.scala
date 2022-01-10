package com.bing.spark.rdd.transform

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object groupByKey {
  def main(args: Array[String]): Unit = {
    val sp=SparkSession.builder().appName("filterTest").master("local[3]").getOrCreate()
    val sc=sp.sparkContext

    groupByKey1(sc)
  }

  //简单1
  def groupByKey1(sc:SparkContext):Unit={
    val rdd1=sc.parallelize(Array((100,"spark"), (100,"hudi"), (90,"hadoop"), (80,"kafka")))
    val rdd2=rdd1.groupByKey()
    rdd2.foreach(println)
  }
}
