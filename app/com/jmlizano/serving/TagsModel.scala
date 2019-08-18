package com.jmlizano.serving

import com.jmlizano.controllers.PostFormInput
import com.jmlizano.SparkSessionWrapper
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions.col
import play.api.libs.json.{Format, Json}


object TagsModel extends SparkSessionWrapper {

  case class tagsResult(tags: Seq[String])

  object tagsResult {
    /**
      * Mapping to read/write a tagsResult out as a JSON value.
      */
    implicit val format: Format[tagsResult] = Json.format
  }

  val model = PipelineModel.read.load("model")
  import spark.implicits._

  def predict(post: PostFormInput): tagsResult = {
    model
      .transform(Seq(post).toDF)
      .select(col("predictionLabel").as("tags"))
      .as[tagsResult]
      .collect()(0)
  }
}
