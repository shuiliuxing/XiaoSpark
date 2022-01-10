package com.bing.spark.rdd.transform

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object aggregate {
  def main(args: Array[String]): Unit = {
    val sp=SparkSession.builder().appName("aggregateTest").master("local").getOrCreate()
    val sc=sp.sparkContext

    aggregate2(sc)
  }

  def aggregate1(sc:SparkContext):Unit={
    val rdd1=sc.parallelize(List(1,2,3,4,5,6,7,8),2)
    val rdd2=rdd1.aggregate(3)(seqOp, combOp)
    println(rdd2)

  }

  def seqOp(a:Int, b:Int):Int={
    println("执行seqOP："+a+"\t"+b)
    math.min(a,b)
  }

  def combOp(a:Int, b:Int):Int={
    println("执行combOp："+a+"\t"+b)
    a+b
  }

  def aggregate2(sc: SparkContext):Unit={
    val rdd1 = sc.parallelize(Seq(("a", 2), ("a", 5), ("a", 4), ("b", 5), ("c", 3), ("b", 3), ("c", 6), ("a", 8)), 4)
    val r1 = rdd1.aggregate((0))(
      (u, c) => (u + c._2),
      (r1, r2) => (r1 + r2)
    )
    println(r1)
  }

  def aggregate3(sc:SparkContext):Unit={
    val rdd1=sc.parallelize(
                              Seq(
                                    ("a",2), ("a",5), ("a",4), ("b",5),
                                    ("c",3), ("b",3), ("c",6), ("a",8)
                              ),
                    1
                           )
    val rdd2=rdd1.aggregate((0, 0))(
      (u,c) => (u._1+1, u._2+c._2),
      (r1,r2) => (r1._1+r2._1, r1._2+r2._2)
    )
    println(rdd2)
  }
}
