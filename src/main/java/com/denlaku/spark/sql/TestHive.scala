package com.denlaku.spark.sql

import org.apache.spark.sql.SparkSession

object TestHive {
  def main(args: Array[String]): Unit = {
    var ss = SparkSession.builder()
      .appName("Java Spark Hive Example")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate();
    ss.sql("select * from teacher").show()
  }
}