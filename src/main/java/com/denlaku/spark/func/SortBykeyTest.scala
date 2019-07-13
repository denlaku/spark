package com.denlaku.spark.func

import com.denlaku.spark.Calc
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions

object SortBykeyTest extends Calc {
  def main(args: Array[String]): Unit = {
    val list = List((99, "张三丰"), (96, "东方不败"), (166, "林平之"), (98, "聂风"))
    sc.parallelize(list).sortByKey(true).foreach(tuple => println(tuple._2 + "->" + tuple._1))
  }
}