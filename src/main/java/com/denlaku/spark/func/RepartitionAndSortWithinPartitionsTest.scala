package com.denlaku.spark.func

import scala.collection.mutable.ListBuffer
import org.apache.spark.HashPartitioner
import com.denlaku.spark.Calc
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions

object RepartitionAndSortWithinPartitionsTest extends Calc {
  def main(args: Array[String]): Unit = {
    val list = List(1, 4, 55, 66, 33, 48, 23)
    val listRDD = sc.parallelize(list, 1)
    listRDD.map(num => (num, num))
      .repartitionAndSortWithinPartitions(new HashPartitioner(2))
      .mapPartitionsWithIndex((index, iterator) => {
        val listBuffer: ListBuffer[String] = new ListBuffer
        while (iterator.hasNext) {
          listBuffer.append(index + "_" + iterator.next())
        }
        listBuffer.iterator
      }, false)
      .foreach(println(_))
  }
}