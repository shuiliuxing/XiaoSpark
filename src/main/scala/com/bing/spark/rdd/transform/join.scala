package com.bing.spark.rdd.transform

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object join {
  def main(args: Array[String]): Unit = {
    val sp=SparkSession.builder().appName("joinTest").master("local[3]").getOrCreate()
    val sc=sp.sparkContext

    joinMethod(sc)
  }

  //join
  def joinMethod(sc:SparkContext):Unit={
    val rdd1=sc.makeRDD(
      Array(
        ("1",("Spark","a")),
        ("2",("Hadoop","b")),
        ("3",("Scala","c")),
        ("4",("Java","d"))
      )
    )
    val rdd2=sc.makeRDD(
      Array(
        ("1",("30K","3G")),
        ("2",("15K","4G")),
        ("3",("25K","5G")),
        ("5",("10K","6G"))
      )
    )
    val joinRdd=rdd1.leftOuterJoin(rdd2)
          //.map(line=>line.toString().replace("(","").replace(")","").replace("Some","").replace("None","-"))
    joinRdd.foreach(println)
  }
}
