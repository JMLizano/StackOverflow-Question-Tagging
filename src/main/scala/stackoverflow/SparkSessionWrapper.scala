package stackoverflow

import org.apache.spark.sql.SparkSession


trait SparkSessionWrapper {

  lazy val spark = SparkSession.builder
    .appName("StackoverflowQuestions")
    .getOrCreate()

}
