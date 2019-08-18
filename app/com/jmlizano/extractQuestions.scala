package com.jmlizano

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._


object extractQuestions {

  val postSchema = StructType(Seq(
    StructField("_Id", IntegerType, nullable = true),
    StructField("_PostTypeId", IntegerType, nullable = true),
    StructField("_ParentId", IntegerType, nullable = true),
    StructField("_AcceptedAnswerId", IntegerType, nullable = true),
    StructField("_CreationDate", StringType, nullable = true),
    StructField("_ClosedDate", StringType, nullable = true),
    StructField("_CommunityOwnedDate", StringType, nullable = true),
    StructField("_Score", IntegerType, nullable = true),
    StructField("_ViewCount", IntegerType, nullable = true),
    StructField("_Body", StringType, nullable = true),
    StructField("_OwnerUserId", IntegerType, nullable = true),
    StructField("_OwnerDisplayName", StringType, nullable = true),
    StructField("_LastEditorUserId", IntegerType, nullable = true),
    StructField("_LastEditorDisplayName", StringType, nullable = true),
    StructField("_LastEditDate", StringType, nullable = true),
    StructField("_LastActivityDate", StringType, nullable = true),
    StructField("_Title", StringType, nullable = true),
    StructField("_Tags", StringType, nullable = true),
    StructField("_AnswerCount", IntegerType, nullable = true),
    StructField("_CommentCount", IntegerType, nullable = true),
    StructField("_FavoriteCount", IntegerType, nullable = true)
  ))

  val postColumnNames = Seq("Id", "AcceptedAnswerId", "CreationDate", "ClosedDate", "CommunityOwnedDate", "Score",
    "ViewCount", "Body", "OwnerUserId", "OwnerDisplayName", "LastEditorUserId", "LastEditorDisplayName", "LastEditDate",
    "LastActivityDate", "Title", "AnswerCount", "CommentCount", "FavoriteCount","Year", "Month", "label")


  def readXml(path: String, rootTag: String, rowTag: String,  schema: StructType, spark: SparkSession): DataFrame = {
    spark
      .read
      .format("com.databricks.spark.xml")
      .option("rowTag", rowTag)
      .schema(schema)
      .load(path)
  }

  def getQuestions(path: String, spark: SparkSession) = {
    import spark.implicits._

    // Repartition by Year,Month. 10 years data x 12months ~ 120 partitions, which is around 3xCores in the cluster I am using
    // Explicily set 120 as num partitions, since spark minimum default is 200 (can also be controlled through spark.sql.shuffle.partitions
    val posts = readXml(path, "posts", "row", postSchema, spark)
      .where("_PostTypeId = 1 AND _Tags is not null AND _CreationDate is not null")
      .drop("_PostTypeId")
      .drop("_ParentId")
      .drop("t") // Hack to make spark-xml work
      .withColumn("_CreationDate", to_date($"_CreationDate", "yyyy-MM-dd'T'HH:mm:SS.SSS"))
      .withColumn("Year", year($"_CreationDate"))
      .withColumn("Month", month($"_CreationDate"))
      .withColumn("_ClosedDate", to_date($"_ClosedDate", "yyyy-MM-dd'T'HH:mm:SS.SSS"))
      .withColumn("_CommunityOwnedDate", to_date($"_CommunityOwnedDate", "yyyy-MM-dd'T'HH:mm:SS.SSS"))
      .withColumn("_LastEditDate", to_date($"_LastEditDate", "yyyy-MM-dd'T'HH:mm:SS.SSS"))
      .withColumn("_LastActivityDate", to_date($"_LastActivityDate", "yyyy-MM-dd'T'HH:mm:SS.SSS"))

    Feature.splitTags(inputCol = "_Tags")
      .transform(posts)
      .drop("_Tags")
      .toDF(postColumnNames: _*)
      .repartition(120, $"Year", $"Month")
  }

  def saveQuestions(questions: DataFrame, path: String) = {
    questions
      .write
      .mode("overwrite")
      .partitionBy("Year", "Month")
      .parquet(path)
  }

}
