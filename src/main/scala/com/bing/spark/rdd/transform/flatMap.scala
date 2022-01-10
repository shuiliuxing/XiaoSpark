package com.bing.spark.rdd.transform

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object flatMap {
  def main(args: Array[String]): Unit = {
    val sp=SparkSession.builder().appName("flatMapTest").master("local[3]").getOrCreate()
    val sc=sp.sparkContext

    flatMapMethod(sc)
  }

  def flatMapMethod(sc:SparkContext):Unit={
    val fileRdd=sc.parallelize(Array("a b","d e","x y"))
    val fm=fileRdd.flatMap(_.split(" "))
    fm.foreach(println)
  }
}
