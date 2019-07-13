package com.denlaku.spark.test

object OptionTest {
  def main(args: Array[String]): Unit = {
    var o:Option[String] = None;
    var ge = o.getOrElse {"++"};
    println(ge)
  }
}