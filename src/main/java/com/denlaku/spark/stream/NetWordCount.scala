package com.denlaku.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.SparkSession

object NetWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sparkContext = new SparkContext(conf)
    val sc = new StreamingContext(sparkContext, Seconds(3))
    
    /**
     * 数据的输入
     */
    val inDStream: ReceiverInputDStream[String] = sc.socketTextStream("10.10.10.11", 9999)
    inDStream.print()
    /**
     * 数据的处理
     */
    val resultDStream: DStream[(String, Int)] = inDStream.flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _)
    /**
     * 数据的输出
     */
    resultDStream.print()

    /**
     * 启动应用程序
     */
    sc.start()
    sc.awaitTermination()
    sc.stop()
  }
}