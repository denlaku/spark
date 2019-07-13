package com.denlaku.spark.func

import com.denlaku.spark.Calc

object SampleTest extends Calc {
  def main(args: Array[String]): Unit = {
    val list = 1 to 100
    var l = 2 until 100;
    val listRDD = sc.parallelize(list)
    listRDD.sample(false, 0.05, 10).foreach(println)
  }
}

object Sam {
  
}