package com.denlaku.spark.func

import org.apache.spark.sql.SparkSession
import scala.io.Source
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object WordCount {
  def main(args: Array[String]): Unit = {

//    spark.sparkContext.parallelize(Seq(1, 2, 3, 4)).map(value => value * 2).foreach(println); ;
//
//    spark.sparkContext.parallelize(Seq(1, 2, 3, 4)).collect().foreach(println);
    
    var filePath = "C:/Users/User/Desktop/temp/wordCount.txt";
    
//    wordCountByScala(filePath);
//    wordCountBySparkRDD(filePath);
    wordCountBySparkDataFrame(filePath);

  }
  
  def wordCountBySparkDataFrame(file: String): Unit = {
     val spark = SparkSession.builder().appName("name").master("local[*]").getOrCreate();
     var df = spark.read.format("csv").load(file).toDF("id", "name", "lang", "year")
     df.show()
     df.printSchema()
     df.select("id")
     .union(df.select("name"))
     .union(df.select("lang"))
     .union(df.select("year"))
     .toDF("word").groupBy("word")
     .count().show()
  }
  
  def wordCountBySparkRDD(file: String): Unit = {
     val spark = SparkSession.builder().appName("name").master("local[*]").getOrCreate();
     var rdd = spark.sparkContext.textFile(file)
     .flatMap(line => line.split(","))
     .map(word => (word, 1))
     .reduceByKey(_ + _)
     rdd.foreach(println)
  }
  
  def wordCountByScala(file: String): Unit = {
    var wc = Source.fromFile(file).getLines()
    .toArray.flatMap(line => line.split(","))
    .map(word => (word, 1)).groupBy(_._1)
    .map(value => (value._1, value._2.map(cell => cell._2).sum));
    wc.foreach(println);
  }
}