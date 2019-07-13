package com.denlaku.spark.func

import com.denlaku.spark.Calc

object IntersectionTest extends Calc {
  def main(args: Array[String]): Unit = {
    val list1 = List(1, 2, 3, 4)
    val list2 = List(3, 4, 5, 6)
    val list1RDD = sc.parallelize(list1)
    val list2RDD = sc.parallelize(list2)
    list1RDD.intersection(list2RDD).foreach(println(_))
  }
}