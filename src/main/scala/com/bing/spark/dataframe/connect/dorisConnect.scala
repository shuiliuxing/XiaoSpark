package com.bing.spark.dataframe.connect

import org.apache.spark.sql.SparkSession

object dorisConnect {
  def main(args: Array[String]): Unit = {
     val sp=SparkSession.builder().appName("connectTest").master("local").getOrCreate()

      dorisRead(sp)
  }

  def dorisRead(sp:SparkSession):Unit={
    val df=sp.read.format("doris")
                  .option("doris.fenodes", "192.168.5.201:8030")
                  .option("doris.table.identifier", "example_db.t_people_info")
                  .option("user", "root")
                  .option("password", "")
                  .load()
    df.show(5)
  }

  def dorisWrite(sp:SparkSession):Unit={
    import sp.implicits._
    val df=List(
      (1001, "zhangsan", 21),
      (1002, "lisi", 22),
      (1003, "wangwu", 23)
    ).toDF("id", "name", "age")

    df.write.format("doris")
      .option("doris.fenodes", "192.168.5.201:8030")
      .option("doris.table.identifier", "example_db.t_people_info")
      .option("user", "root")
      .option("password", "")
      .mode("append")
      .save()
  }
}
