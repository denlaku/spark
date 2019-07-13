package com.denlaku.spark.func

import scala.collection.mutable.ListBuffer
import com.denlaku.spark.Calc

object MapPartitionsWithIndexTest extends Calc {
  def main(args: Array[String]): Unit = {
    val list = List(1, 2, 3, 4, 5, 6, 7, 8)
    sc.parallelize(list, 2).mapPartitionsWithIndex((index, iterator) => {
      val listBuffer: ListBuffer[String] = new ListBuffer
      while (iterator.hasNext) {
        listBuffer.append(index + "_" + iterator.next())
      }
      listBuffer.iterator
    }, true)
      .foreach(println(_))
  }
}