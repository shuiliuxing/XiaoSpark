package com.bing.spark.rdd.transform

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object repartition {
  def main(args: Array[String]): Unit = {
    val sp=SparkSession.builder().appName("filterTest").master("local[3]").getOrCreate()
    val sc=sp.sparkContext

    repartition1(sc)
  }

  def repartition1(sc:SparkContext):Unit={
    val rdd1=sc.makeRDD(1 to 16, 4)
    rdd1.foreach(println)
    println("第1个分区是："+rdd1.getNumPartitions)
    val rdd2=rdd1.repartition(3)
    rdd2.foreach(println)
    println("第2个分区是："+rdd2.getNumPartitions)
  }
}
