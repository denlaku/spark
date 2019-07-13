package com.denlaku.spark.func

import com.denlaku.spark.Calc

object MapTest extends Calc {
  def main(args: Array[String]): Unit = {
    val list = List("张无忌", "赵敏", "周芷若")
    val listRDD = sc.parallelize(list)
    val nameRDD = listRDD.map(name => "Hello: " + name)
    list.synchronized {
    }
    nameRDD.foreach(println)
  }
}