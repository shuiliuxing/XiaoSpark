package com.bing.spark.dataframe.connect

import org.apache.spark.sql.SparkSession

object fileConnect {
  def main(args: Array[String]): Unit = {
    val sp=SparkSession.builder().appName("filterTest").master("local[3]").getOrCreate()
    csvWrite(sp)
  }

  def csvRead(sp:SparkSession):Unit={
    val df=sp.read.format("csv")
                  .option("header", true)
                  .load("E:\\data\\spark\\dataframe\\test\\read\\movies_1.csv")
    df.show()
  }

  def csvWrite(sp:SparkSession):Unit={
    import sp.implicits._
    val df=List(
      (5,"Roman", "Comedy"),
      (6,"Little Girl", "Comedy")
    ).toDF("id", "name", "style")
    df.write.format("csv")
                  .option("path","E:\\data\\spark\\dataframe\\test\\write\\wr_movies_1.csv")
                  .mode("append")   // append / overwirte
                  .save()
  }

}
