package com.denlaku.spark.stream

import com.denlaku.spark.Calc
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object OnlineBlackListFilter extends Calc {
  def main(args: Array[String]): Unit = {
    
    val blackList = Array(("Spy", true), ("Cheater", true))
    
    val ssc = new StreamingContext(sc, Seconds(30))
    
    val blackListRdd = ssc.sparkContext.parallelize(blackList, 8)
    
    val adsClickStream  = ssc.socketTextStream("10.10.10.11", 9999)
    
    val adsClickStreamFormatted = adsClickStream.map(ads => (ads.split(" ")(1), ads))
    
    adsClickStreamFormatted.transform(userClickRdd => {
      val joinBlackListRdd = userClickRdd.leftOuterJoin(blackListRdd);
      
      val validClicked = joinBlackListRdd.filter(joinItem => {
        println(joinItem._1, joinItem._2._1, joinItem._2._2)
        if (joinItem._2._2.getOrElse(false)) {
          false
        } else {
          true
        }
        
        
      })
      validClicked.map(validClick => (validClick._2._1))
    }).print()
    
    ssc.start()
    ssc.awaitTermination()
  }
}