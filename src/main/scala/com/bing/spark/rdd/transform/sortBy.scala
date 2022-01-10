package com.bing.spark.rdd.transform

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object sortBy {
  def main(args: Array[String]): Unit = {
    val sp=SparkSession.builder().appName("reduceByKeyTest").master("local[3]").getOrCreate()
    val sc=sp.sparkContext

    sortByMethod1(sc)
  }

  // 顺序
  def sortByMethod(sc:SparkContext):Unit={
    val rdd1 = sc.makeRDD(List(2,5,1,8,3,3),3)
    val rdd2 = rdd1.sortBy(x => x)
    rdd2.collect().foreach(println)
  }

  // 逆序
    def sortByMethod1(sc:SparkContext):Unit={
    val rdd1 = sc.makeRDD(List(2,5,1,8,3,3),3)
    val rdd2 = rdd1.sortBy(x => x, false)
    rdd2.collect().foreach(println)
  }
}
