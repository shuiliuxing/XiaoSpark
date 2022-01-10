package com.bing.spark.rdd.transform

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object filter {
  def main(args: Array[String]): Unit = {
    val sp=SparkSession.builder().appName("filterTest").master("local[3]").getOrCreate()
    val sc=sp.sparkContext

    filter1(sc)
  }

    //简单1
  def filter1(sc:SparkContext):Unit={
    val rdd=sc.textFile("D:\\spark\\rdd\\transformation\\filter\\one.txt")
    val filterRdd=rdd.filter(line=>line.contains("bb"))
    filterRdd.foreach(println)
  }
}
