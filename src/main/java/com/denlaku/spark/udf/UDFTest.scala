package com.denlaku.spark.udf

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import com.denlaku.spark.Calc
import org.apache.spark.sql.SparkSession

object UDFTest {
  def main(args: Array[String]): Unit = {
    
    var ss = SparkSession.builder()
      .appName("Java Spark Hive Example")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    /**
      * 第一步 创建程序入口
      */
    val conf = new SparkConf().setAppName("AralHotProductSpark")
    val sc = new SparkContext(conf)
//    SparkSession.builder().enableHiveSupport().getOrCreate(sc);
    val hiveContext = new HiveContext(sc)
    hiveContext.udf.register("get_distinct_city",GetDistinctCityUDF)
    hiveContext.udf.register("get_product_status",(str:String) => {
      var status = 0
      for(s <- str.split(",")){
        if (s.contains("product_status")) {
          status = s.split(":")(1).toInt
        }
      }
    })
}
}