package com.jmlizano

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  val spark = SparkSession.builder
    .appName("StackoverflowQuestions")
    .master("local[1]")
    .getOrCreate()
}
