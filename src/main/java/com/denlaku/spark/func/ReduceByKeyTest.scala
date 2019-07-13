package com.denlaku.spark.func

import com.denlaku.spark.Calc
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object ReduceByKeyTest extends Calc {
  def main(args: Array[String]): Unit = {
    val list = List(("武当", 99), ("少林", 97), ("武当", 89), ("少林", 77))
    val mapRDD = sc.parallelize(list)
    var result = mapRDD.reduceByKey((x, y) => x+ y)
    result.foreach(t => println(t._1 + ": " + t._2))
  }
}