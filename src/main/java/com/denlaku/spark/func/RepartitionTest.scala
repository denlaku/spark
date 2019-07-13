package com.denlaku.spark.func

import com.denlaku.spark.Calc

object RepartitionTest extends Calc {
  def main(args: Array[String]): Unit = {
    val list = List(1, 2, 3, 4)
    val listRDD = sc.parallelize(list, 1)
    listRDD.repartition(2).foreach(println(_))
  }
}