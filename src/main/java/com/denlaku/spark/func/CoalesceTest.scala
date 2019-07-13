package com.denlaku.spark.func

import com.denlaku.spark.Calc

object CoalesceTest extends Calc {
  def main(args: Array[String]): Unit = {
    val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9)
    sc.parallelize(list, 3).coalesce(1).foreach(println(_))
  }
}