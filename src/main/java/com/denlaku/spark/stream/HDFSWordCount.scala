package com.denlaku.spark.stream

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds

object HDFSWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    val sc = new StreamingContext(conf, Seconds(2))

    val inDStream: DStream[String] = sc.textFileStream("hdfs://10.10.10.11:9000/streaming")
    val resultDStream: DStream[(String, Int)] = inDStream.flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _)
    resultDStream.print()

    sc.start()
    sc.awaitTermination()
    sc.stop()
  }
}