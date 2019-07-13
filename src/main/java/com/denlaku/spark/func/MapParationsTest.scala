package com.denlaku.spark.func

import scala.collection.mutable.ListBuffer
import com.denlaku.spark.Calc

object MapParationsTest extends Calc {
  def main(args: Array[String]): Unit = {
    val list = List(1, 2, 3, 4, 5, 6)
    val listRDD = sc.parallelize(list, 4)

    listRDD.mapPartitions(iterator => {
      val newList: ListBuffer[String] = ListBuffer()
      while (iterator.hasNext) {
        newList.append("hello: " + iterator.next())
      }
      println(newList)
      newList.toIterator
    }).foreach(println)
  }
}