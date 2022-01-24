package com.bing.spark.dataframe.connect

import java.util.Properties

import org.apache.spark.sql.SparkSession

object mysqlConnect {
  def main(args: Array[String]): Unit = {
    val sp=SparkSession.builder().appName("connectTest").master("local").getOrCreate()

    mysqlRead2(sp)
  }

  def mysqlRead(sp:SparkSession):Unit={
    val df=sp.read.format("jdbc")
                  .option("driver", "com.mysql.jdbc.Driver")
                  .option("url", "jdbc:mysql://122.152.200.71:3306/test2")
                  .option("dbtable", "t_people_info")
                  .option("user", "root")
                  .option("password", "")
                .load()
    df.show(5)
  }

  def mysqlWrite(sp:SparkSession):Unit={
    import sp.implicits._
    val df=List(
      (1007, "qiqi", 27),
      (1008, "liuba", 28)
    ).toDF("id", "name", "age")

    df.write.format("jdbc")
                  .option("driver", "com.mysql.jdbc.Driver")
                  .option("url", "jdbc:mysql://122.152.200.71:3306/test2")
                  .option("dbtable", "t_people_info")
                  .option("user", "root")
                  .option("password", "")
                  .mode("append")
                  .save()

  }

  def mysqlRead2(sp:SparkSession):Unit={
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "")
    val df = sp.read.jdbc("jdbc:mysql://122.152.200.71:3306/test2", "t_people_info", properties)
    df.show(5)

  }


}
