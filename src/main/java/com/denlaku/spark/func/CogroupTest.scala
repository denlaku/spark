package com.denlaku.spark.func

import com.denlaku.spark.Calc
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object CogroupTest extends Calc {
  def main(args: Array[String]): Unit = {
    val list1 = List((1, "www"), (2, "bbs"))
    val list2 = List((1, "cnblog"), (2, "cnblog"), (3, "very"))
    val list3 = List((1, "com"), (2, "com"), (3, "good"))

    val list1RDD = sc.parallelize(list1)
    val list2RDD = sc.parallelize(list2)
    val list3RDD = sc.parallelize(list3)

    list1RDD.cogroup(list2RDD, list3RDD).foreach(tuple =>
      println(tuple._1 + " " + tuple._2._1 + " " + tuple._2._2 + " " + tuple._2._3))
  }
}