package com.bing.spark.dataframe.operate

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object filter {
  def main(args: Array[String]): Unit ={
    val sp=SparkSession.builder().appName("aggregateTest").master("local").getOrCreate()

    filter3(sp)
  }

  def filter1(sp:SparkSession): Unit ={
    import sp.implicits._
    val sc=sp.sparkContext

    val df = sc.parallelize(Seq(("aa", 1), ("aa", 2), ("bb", 2), ("bb", 3), ("cc", 1))).toDF("id", "num")
    df.filter("id ='aa'").show()
  }

  def filter2(sp:SparkSession): Unit ={
    import sp.implicits._
    val sc=sp.sparkContext

    val df = sc.parallelize(Seq(("aa", 1), ("aa", 2), ("bb", 2), ("bb", 3), ("cc", 1))).toDF("id", "num")
    df.filter("id='aa' and num=1").show()
  }

  def filter3(sp:SparkSession): Unit ={
    import sp.implicits._
    val sc=sp.sparkContext

    val df = sc.parallelize(Seq(("aa", 1), ("aa", 2), ("bb", 2), ("bb", 3), ("cbc", 1))).toDF("id", "num")
    df.filter("id like '%b%'").show()
  }
}
