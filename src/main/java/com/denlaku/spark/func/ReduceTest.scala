package com.denlaku.spark.func

import com.denlaku.spark.Calc

object ReduceTest extends Calc {
  def main(args: Array[String]): Unit = {
    val list = List[Int](1, 2, 3, 4, 5, 6)
    val listRDD = sc.parallelize(list)

    val result = listRDD.reduce((x, y) => x + y)
    println(result)
  }
}