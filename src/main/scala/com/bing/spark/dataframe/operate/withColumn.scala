package com.bing.spark.dataframe.operate

import java.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._

object withColumn {
  def main(args: Array[String]): Unit ={
    val sp=SparkSession.builder().appName("aggregateTest").master("local").getOrCreate()
    withColumn6(sp)
  }

  def withColumn1(sp:SparkSession): Unit ={
    import sp.implicits._
    val sc=sp.sparkContext

    val df = sc.parallelize(Seq(("aa", 1), ("aa", 2), ("bb", 2), ("bb", 3), ("cc", 1))).toDF("id", "num")
    df.withColumn("id", expr("upper(id)")).show()
  }

  def withColumn2(sp:SparkSession): Unit ={
    import sp.implicits._
    val sc=sp.sparkContext

    val df = sc.parallelize(Seq(("aa", 1), ("aa", 2), ("bb", 2), ("bb", 3), ("cc", 1))).toDF("id", "num")
    df.withColumn("id", expr("upper(id)")).show()
  }

  def withColumn3(sp:SparkSession): Unit ={
    import sp.implicits._
    val sc=sp.sparkContext

    val df = sc.parallelize(Seq(("aa_zhang", 1), ("aa_li", 2), ("bb_wang", 2), ("bb_liu", 3), ("cc_shen", 1))).toDF("id", "num")
    val newDF=df.withColumn("id", split(col("id"), "_"))

    var targetDF=newDF
    var index=0
    val colList=List[String]("newId", "name")
    colList.foreach(s=>{
      targetDF=targetDF.withColumn(s, newDF("id")(index))
      index+=1
    })

    targetDF.show
  }

  def withColumn4(sp:SparkSession): Unit ={
    import sp.implicits._
    val sc=sp.sparkContext

    var df=Seq(
      Student(1001, "张三", "F", 20),
      Student(1002, "李四", "M", 16),
      Student(1003, "王五", "M", 21),
      Student(1004, "赵六", "F", 21),
      Student(1005, "周七", "M", 22)
    ).toDF()

    df=df.withColumn("id", df.col("id").cast("String"))
    df=df.withColumnRenamed("id", "bid")
    df.show

  }

  def withColumn5(sp:SparkSession): Unit ={
    import sp.implicits._
    val sc=sp.sparkContext

    val df=Seq("张三_18_广东","李四_19_广西","王五_20_湖南"
    ).toDF("info")
    df.show()

    var newDF=df.withColumn("info", split(col("info"), "_"))
    newDF.show()

    val nameList=List("name", "age", "addr")
    var targetDF=newDF
    var index=0
    nameList.foreach(s=>{
      targetDF=targetDF.withColumn(s, newDF("info")(index))
      index+=1
    })
    targetDF.drop("info").show()
  }

  def withColumn6(sp:SparkSession): Unit ={
    import sp.implicits._
    val sc=sp.sparkContext
    val df=Seq((1001, "zhang", 18), (1002, "li", 19), (1003, "wang", 20)).toDF("id", "name", "age")
    df.show()
    df.withColumn("name", expr("upper(name)")).show()
  }

  case class Student(id:Int, name:String, gender:String, age:Int)

}
