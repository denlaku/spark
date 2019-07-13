package com.denlaku.spark.sql

import com.denlaku.spark.Calc
import org.apache.spark.sql.SQLContext
import java.util.Properties

object TestMysql extends Calc {
  def main(args: Array[String]): Unit = {
    val sqlContext = new SQLContext(sc)
    val url = "jdbc:mysql://localhost:3306/demo?characterEncoding=utf8&useSSL=false&serverTimezone=UTC"
    val table = "employee"
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "denlaku")
    //需要传入MySql的URL、表明、properties（连接数据库的用户名密码）
    val df = sqlContext.read.jdbc(url, table, properties)
    df.createOrReplaceTempView("dbs")
    sqlContext.sql("select * from dbs").show()
  }
}