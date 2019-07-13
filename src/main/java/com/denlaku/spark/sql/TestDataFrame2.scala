package com.denlaku.spark.sql

import com.denlaku.spark.Calc
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object TestDataFrame2 extends Calc {
  def main(args: Array[String]): Unit = {
    var rdd = sc.textFile("C:\\Users\\User\\Desktop\\temp\\people.txt")
    val sqlContext = new SQLContext(sc)
    // 将 RDD 数据映射成 Row，需要 import org.apache.spark.sql.Row
    var rowRDD: RDD[Row] = rdd.map(line => {
      var fields = line.split(",")
      Row(fields(0), fields(1).trim.toInt)
    })
    // 创建 StructType 来定义结构
    val structType: StructType = StructType(
      //字段名，字段类型，是否可以为空
      StructField("name", StringType, true) ::
        StructField("age", IntegerType, true) :: Nil)
    var df = sqlContext.createDataFrame(rowRDD, structType)
    df.createOrReplaceTempView("people")
    sqlContext.sql("select * from people").show()
  }
}