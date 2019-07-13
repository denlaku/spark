package com.denlaku.spark.sql

import com.denlaku.spark.Calc
import org.apache.spark.sql.SQLContext

object DataFrameRead extends Calc {
  def main(args: Array[String]): Unit = {
    val sqlContext = new SQLContext(sc)
    //方式一
    val df1 = sqlContext.read.json(basePath + "people.json")
    df1.show()
//    val df2 = sqlContext.read.parquet(basePath + "222\\part-00000-2d029c42-ebcb-4b5d-8664-26bea7616bfd-c000.snappy.parquet")
//    df2.show()
//    var csvDF = sqlContext.read.csv(basePath + "people.csv").toDF("id", "name", "age")
//    csvDF.show()
//    //  方式二
//    val df3 = sqlContext.read.format("json").load(basePath + "people.json")
//    df3.show()
//    val df4 = sqlContext.read.format("parquet").load(basePath + "222\\part-00000-2d029c42-ebcb-4b5d-8664-26bea7616bfd-c000.snappy.parquet")
//    df4.show()
//    //方式三，默认是parquet格式
//    val df5 = sqlContext.read.load(basePath + "222\\part-00000-2d029c42-ebcb-4b5d-8664-26bea7616bfd-c000.snappy.parquet")
//    df5.show()
  }
}