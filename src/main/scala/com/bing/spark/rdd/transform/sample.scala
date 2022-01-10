package com.bing.spark.rdd.transform

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object sample {
  def main(args: Array[String]): Unit = {
    val sp=SparkSession.builder().appName("filterTest").master("local[3]").getOrCreate()
    val sc=sp.sparkContext

    sample1(sc)
  }

  def sample1(sc:SparkContext):Unit={
    val rdd1=sc.parallelize(1 to 11,3)
    val rdd2=rdd1.sample(false,0.5, 1)
    rdd2.collect().foreach(println)
  }
}
