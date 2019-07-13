package com.denlaku.spark.func

import com.denlaku.spark.Calc

object UnionTest extends Calc {
  def main(args: Array[String]): Unit = {
    val list1 = List(1, 2, 3, 4)
    val list2 = List(3, 4, 5, 6)
    val rdd1 = sc.parallelize(list1)
    val rdd2 = sc.parallelize(list2)
    rdd1.union(rdd2).foreach(println)
  }
}