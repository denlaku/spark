package com.denlaku.spark.func

import com.denlaku.spark.Calc
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object JoinTest extends Calc {
  def main(args: Array[String]): Unit = {
    val list1 = List((1, "东方不败"), (2, "令狐冲"), (3, "林平之"))
    val list2 = List((2, 99), (1, 98), (3, 97))
    val list1RDD = sc.parallelize(list1)
    val list2RDD = sc.parallelize(list2)

//    val rdd3 = sc.broadcast(list2RDD.collect())
    
//    list1RDD.join(sc.parallelize(rdd3.value))

    val joinRDD = list1RDD.join(list2RDD)
    joinRDD.foreach(t => println("学号:" + t._1 + " 姓名:" + t._2._1 + " 成绩:" + t._2._2))
  }
}