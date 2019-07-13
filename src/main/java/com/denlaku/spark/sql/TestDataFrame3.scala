package com.denlaku.spark.sql

import org.apache.spark.sql.SQLContext
import com.denlaku.spark.Calc

object TestDataFrame3 extends Calc{
  def main(args: Array[String]): Unit = {
//    var rdd = sc.textFile("C:\\Users\\User\\Desktop\\temp\\people.json")
    val sqlContext = new SQLContext(sc)
    var df = sqlContext.read.json("C:\\Users\\User\\Desktop\\temp\\people.json")
    df.createOrReplaceTempView("people")
    sqlContext.sql("select * from people").show()
  }
}