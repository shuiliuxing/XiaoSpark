package com.bing.spark.sql.operate

import org.apache.spark.sql.SparkSession

object HiveOperate {

  def main(args: Array[String]): Unit = {
     val sp=SparkSession.builder()
                        .appName("UpdateSymptom")
                        .master("local")
                        .config("spark.hadoop.hive.metastore.uris", "thrift://192.168.5.203:9083")
                        .config("spark.hadoop.fs.defaultFS", "hdfs://192.168.5.201:8020")
                        .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                        .config("spark.debug.maxToStringFields","100")
                        .enableHiveSupport()
                        .getOrCreate()
    hiveRead(sp)
  }

  def hiveRead(sp:SparkSession):Unit={
    val df=sp.sql("select * from test.t_people limit 10")
    df.show()
  }
}

/*
    需复制core-site.xml、hdfs-site.xml到resources目录下
    HA模式
    val sp=SparkSession.builder()
                       .config("spark.hadoop.hive.metastore.uris", "thrift://192.168.5.203:9083")
                       .config("spark.hadoop.fs.defaultFS", "hdfs://hacluster")
                       .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                       .config("hive.exec.dynamici.partition", true)
                       .config("hive.exec.dynamic.partition.mode", "nonstrict")
                       .config("spark.debug.maxToStringFields","100")
                       .appName("connectTest")
                       .master("local")
                       .getOrCreate()

 */