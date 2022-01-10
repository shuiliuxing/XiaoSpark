package com.bing.spark.rdd.transform

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object sortByKey {
  def main(args: Array[String]): Unit = {
    val sp=SparkSession.builder().appName("sortByKeyTest").master("local").getOrCreate()
    val sc=sp.sparkContext

    sortByKey1(sc)
  }

  def sortByKey1(sc:SparkContext):Unit={
    val rdd=sc.makeRDD(List(
                              (5, "hello"),
                              (3, "world"),
                              (4, "good"),
                              (1, "nothing")
                            )
                      );
    val rdd2=rdd.sortByKey().collect()
    rdd2.foreach(println)
  }
}
