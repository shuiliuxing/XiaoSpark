package com.bing.spark.rdd.transform

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object sample {
  def main(args: Array[String]): Unit = {
    val sp=SparkSession.builder().appName("filterTest").master("local[3]").getOrCreate()
    val sc=sp.sparkContext

    sample4(sc)
  }

  def sample1(sc:SparkContext):Unit={
    val rdd1=sc.parallelize(1 to 11,3)
    val rdd2=rdd1.sample(false,0.5, 1)
    rdd2.collect().foreach(println)
  }

  def sample2(sc:SparkContext):Unit={
    val rdd1=sc.parallelize(1 to 10,1)
    val rdd2=rdd1.sample(true,0.5)
    rdd2.collect().foreach(println)
  }

  def sample3(sc:SparkContext):Unit={
    val rdd1=sc.parallelize(Seq(1, 2, 3, 5, 6, 7, 8, 9, 10))
    val rdd2=rdd1.sample(false,0.5)
    rdd2.collect().foreach(println)
  }

  def sample4(sc:SparkContext):Unit={
    val rdd1=sc.parallelize(Seq(1, 2, 3, 5, 6, 7, 8, 9, 10))
    val rdd2=rdd1.takeSample(false, 4)
    rdd2.foreach(println)
  }
}
