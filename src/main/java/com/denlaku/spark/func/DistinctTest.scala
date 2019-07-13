package com.denlaku.spark.func

import com.denlaku.spark.Calc

object DistinctTest extends Calc {
  def main(args: Array[String]): Unit = {
    val list = List(1, 1, 2, 2, 3, 3, 4, 5)
    sc.parallelize(list).distinct().foreach(println)
  }
}