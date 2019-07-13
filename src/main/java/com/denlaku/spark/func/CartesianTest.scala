package com.denlaku.spark.func

import com.denlaku.spark.Calc

object CartesianTest extends Calc {
  def main(args: Array[String]): Unit = {
    val list1 = List("A", "B")
    val list2 = List(1, 2, 3)
    val list1RDD = sc.parallelize(list1)
    val list2RDD = sc.parallelize(list2)
    list1RDD.cartesian(list2RDD).foreach(t => println(t._1 + "->" + t._2))
  }
}