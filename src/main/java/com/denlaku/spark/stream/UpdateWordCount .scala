package com.denlaku.spark.stream

import org.apache.spark.streaming.dstream.{ DStream, ReceiverInputDStream }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.streaming.{ Seconds, StreamingContext }

object UpdateWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    System.setProperty("HADOOP_USER_NAME", "hadoop")
    val sparkContext = new SparkContext(conf)

    val sc = new StreamingContext(sparkContext, Seconds(2))

    sc.checkpoint("hdfs://10.10.10.11:9000/streaming")
    val inDStream: ReceiverInputDStream[String] = sc.socketTextStream("10.10.10.11", 9999)

    val resultDStream: DStream[(String, Int)] = inDStream.flatMap(_.split(","))
      .map((_, 1))
      .updateStateByKey((values: Seq[Int], state: Option[Int]) => {
        val currentCount: Int = values.sum
        val lastCount: Int = state.getOrElse(0)
        Some(currentCount + lastCount)
      })
    resultDStream.print()

    sc.start()
    sc.awaitTermination()
    sc.stop()
  }
}