package com.bing.spark.dataframe.operate

import org.apache.spark.sql.SparkSession

/**
 * 正常情况下，当我们使用了聚合算子，后面就无法在使用其他聚合算子
  而agg可以使我们同时获取多个聚合运算结果
 */
object agg {
  def main(args: Array[String]): Unit ={
    val sp=SparkSession.builder().appName("aggregateTest").master("local").getOrCreate()

    agg2(sp)
  }


  def agg1(sp:SparkSession): Unit ={
    import sp.implicits._
    val sc=sp.sparkContext

    val stuDF=Seq(
      Student(1001, "张三", "F", 20),
      Student(1002, "李四", "M", 16),
      Student(1003, "王五", "M", 21),
      Student(1004, "赵六", "F", 21),
      Student(1005, "周七", "M", 22)
    ).toDF()

    stuDF.show()

    import org.apache.spark.sql.functions._
    //同样也可以这样写
    //stuDF.groupBy("gender").agg(max("age"),min("age"),avg("age"),count("id")).show()
    stuDF.groupBy("gender").agg("age"->"max","age"->"min","age"->"avg","id"->"count").show()
  }

  def agg2(sp:SparkSession): Unit ={
    import sp.implicits._
    val sc=sp.sparkContext

    val stuDF=Seq(
      Student(1001, "张三", "F", 10),
      Student(1002, "李四", "M", 6),
      Student(1003, "王五", "M", 9),
      Student(1004, "赵六", "F", 5),
      Student(1005, "周七", "M", 15)
    ).toDF()

    stuDF.show()

    import org.apache.spark.sql.functions._
    //同样也可以这样写
    //stuDF.groupBy("gender").agg(max("age"),min("age"),avg("age"),count("id")).show()
    val newDF=stuDF.groupBy("gender").agg("bid"->"count","age"->"max", "age"->"avg")
    newDF.withColumnRenamed("count(bid)", "count_bid")
      .withColumnRenamed("max(age)", "max_age")
      .withColumnRenamed("avg(age)", "avg_age").show()
  }

}

case class Student(bid:Int, name:String, gender:String, age:Int)
