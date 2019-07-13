package com.denlaku.spark.func

import com.denlaku.spark.Calc
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object AggregateByKeyTest extends Calc {
  def main(args: Array[String]): Unit = {
    val list = List("you,jump", "i,jump")
    sc.parallelize(list)
      .flatMap(_.split(","))
      .map((_, 1))
      .aggregateByKey(0)(_ + _, _ + _)
      .foreach(tuple => println(tuple._1 + "->" + tuple._2))
  }
}