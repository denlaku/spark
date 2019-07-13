package com.denlaku.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

trait Calc {
//  private val conf: SparkConf = new SparkConf().setAppName("TestTransformation").setMaster("local")
//  protected val sc = new SparkContext(conf)
  private[spark] val basePath = "D:\\myspace\\temp\\spark\\"
  protected val ss = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[2]").config("spark.ui.enabled", false).getOrCreate();
  protected val sc =  ss.sparkContext;
}