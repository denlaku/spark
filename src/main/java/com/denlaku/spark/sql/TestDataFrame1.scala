package com.denlaku.spark.sql

import com.denlaku.spark.Calc
import org.apache.spark.sql.SQLContext

case class People(var name:String,var age:Int)

object TestDataFrame1 extends Calc{
  def main(args: Array[String]): Unit = {
    var rdd = sc.textFile("D:\\myspace\\temp\\people.txt")
    var  peopleRDD = rdd.map(line => People(line.split(",")(0), line.split(",")(1).trim.toInt))
    val context = new SQLContext(sc)
    import context.implicits._
    var df = peopleRDD.toDF
    df.createOrReplaceTempView("people");
    context.sql("select * from people").show()
  }
}