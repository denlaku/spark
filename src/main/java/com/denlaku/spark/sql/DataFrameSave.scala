package com.denlaku.spark.sql

import com.denlaku.spark.Calc
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext

object DataFrameSave extends Calc {
  def main(args: Array[String]): Unit = {
    val sqlContext = new SQLContext(sc)
    val df1 = sqlContext.read.json(basePath + "people.json")
    // 方式一
    df1.write.json(basePath + "json")
    df1.write.parquet(basePath + "parquet")
    df1.write.csv(basePath + "csv")
    df1.write.orc(basePath + "orc")
    // 方式二
    df1.write.format("json").save(basePath + "fmt_json")
    df1.write.format("parquet").save(basePath + "fmt_parquet")
    df1.write.format("csv").save(basePath + "fmt_csv")
    df1.write.format("orc").save(basePath + "fmt_orc")
    // 方式三
    df1.write.save(basePath + "save")

  }
}