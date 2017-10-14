package com.levelmoney.scalaedn

import org.apache.spark.sql.SparkSession

trait SparkTestContext {

  implicit val sparkSession = SparkSession.builder().appName("Unit tests context").master("local").getOrCreate()

}
