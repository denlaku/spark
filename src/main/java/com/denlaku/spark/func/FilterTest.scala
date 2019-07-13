package com.denlaku.spark.func

import com.denlaku.spark.Calc

object FilterTest extends Calc {
  def main(args: Array[String]): Unit = {
    val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val listRDD = sc.parallelize(list)

    // 创建RDD的方式
    // sc.emptyRDD
    // sc.hadoopFile("")
    // sc.newAPIHadoopFile("")
    // sc.makeRDD(list)
    // sc.sequenceFile("", null, null);
    // sc.union(listRDD);
    // sc.textFile("", 2);

    listRDD.filter(num => num % 2 == 0).foreach(println(_))
    
  }
}