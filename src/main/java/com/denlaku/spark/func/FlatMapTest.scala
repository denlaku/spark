package com.denlaku.spark.func

import com.denlaku.spark.Calc

object FlatMapTest extends Calc {
  def main(args: Array[String]): Unit = {
    val list = List("张无忌 赵敏", "宋青书 周芷若")
    val listRDD = sc.parallelize(list)

    val nameRDD = listRDD.flatMap(line => line.split(" ")).map(name => "Hello " + name)
    nameRDD.foreach(println)
  }
}