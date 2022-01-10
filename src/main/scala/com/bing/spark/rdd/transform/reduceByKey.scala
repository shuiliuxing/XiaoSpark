package com.bing.spark.rdd.transform

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object reduceByKey {
  def main(args: Array[String]): Unit = {
    val sp=SparkSession.builder().appName("reduceByKeyTest").master("local[3]").getOrCreate()
    val sc=sp.sparkContext

    reduceByKeyMethod(sc)
  }

  def reduceByKeyMethod(sc:SparkContext):Unit={
    val fileRdd=sc.textFile("E:\\data\\spark\\rdd\\test\\read\\word.txt")

    val rby=fileRdd.flatMap(_.split(" "))
                   .map((_,1))
                   .reduceByKey(_+_)
    rby.foreach(println)
  }
}
