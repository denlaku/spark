package com.denlaku.spark.test

object SetTest {
  def main(args: Array[String]): Unit = {
    var s1 = Set(1, 3)
    var s2 = Set(1, 2)
    var s3 = s1 -- s2
    print(s3)
  }
}