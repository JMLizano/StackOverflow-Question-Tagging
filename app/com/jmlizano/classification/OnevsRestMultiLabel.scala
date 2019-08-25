/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jmlizano.classification

import java.util.UUID

import scala.language.existentials
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml._
import org.apache.spark.ml.classification.{ClassificationModel, Classifier}
import org.apache.spark.ml.param.{Param, ParamMap, ParamPair, Params}
import org.apache.spark.ml.util._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.ml.Hacks._
import org.apache.spark.ml.param.shared.HasWeightCol
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, JObject, _}
import org.json4s.JsonDSL._
import org.json4s.jackson.Serialization


trait ClassifierTypeTrait {
  // scalastyle:off structural.type
  type ClassifierType = Classifier[F, E, M] forSome {
    type F
    type M <: ClassificationModel[F, M]
    type E <: Classifier[F, E, M]
  }
  // scalastyle:on structural.type
}

/**
  * Trait for shared param weightCol. This trait may be changed or
  * removed between minor versions.
  */
@DeveloperApi
trait HasStringPredictionCol extends Params {

  /**
    * Param for weight column name. If this is not set or empty, we treat all instance weights as 1.0.
    * @group param
    */
  final val stringPredictionCol: Param[String] = new Param[String](
    this,
    "stringPredictionCol",
    "Column name for the predictions as Strings")

  /** @group getParam */
  final def getStringPredictionCol: String = $(stringPredictionCol)
}
/**
  * Params for [[com.jmlizano.classification.OneVsRestMulti]].
  */
trait OneVsRestMultiParams extends _ClassifierParams
  with HasWeightCol with ClassifierTypeTrait  with HasStringPredictionCol {

  /**
    * param for the base binary classifier that we reduce multiclass stackoverflow.classification into.
    * The base classifier input and output columns are ignored in favor of
    * the ones specified in [[com.jmlizano.classification.OneVsRestMulti]].
    * @group param
    */
  val classifier: Param[ClassifierType] = new Param(
    this,
    "classifier",
    "base binary classifier")

  /** @group getParam */
  def getClassifier: ClassifierType = $(classifier)

  /** @group setParam */
  def setLabelCol(value: String): this.type = set(labelCol, value)

  /** @group setParam */
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  def setRawPredictionCol(value: String): this.type = set(rawPredictionCol, value)

  /** @group setParam */
  def setStringPredictionCol(value: String): this.type = set(stringPredictionCol, value)
}


object OneVsRestMultiParams extends ClassifierTypeTrait {

  def validateParams(instance: OneVsRestMultiParams): Unit = {
    def checkElement(elem: Params, name: String): Unit = elem match {
      case stage: MLWritable => // good
      case other =>
        throw new UnsupportedOperationException("OneVsRestMulti write will fail " +
          s" because it contains $name which does not implement MLWritable." +
          s" Non-Writable $name: ${other.uid} of type ${other.getClass}")
    }

    instance match {
      case ovrModel: OneVsRestMultiModel => ovrModel.models.foreach(checkElement(_, "model"))
      case _ => // no need to check OneVsRest here
    }

    checkElement(instance.getClassifier, "classifier")
  }

  def saveImpl(
                path: String,
                instance: OneVsRestMultiParams,
                sc: SparkContext,
                extraMetadata: Option[JObject] = None): Unit = {

    val params = instance.extractParamMap().toSeq
    val jsonParams = render(params
      .filter { case ParamPair(p, v) => p.name != "classifier" }
      .map { case ParamPair(p, v) => p.name -> parse(p.jsonEncode(v)) }
      .toList)

    _DefaultParamsWriter.saveMetadata(instance, path, sc, extraMetadata, Some(jsonParams))

    val classifierPath = new Path(path, "classifier").toString
    instance.getClassifier.asInstanceOf[MLWritable].save(classifierPath)
  }

  def loadImpl(
                path: String,
                sc: SparkContext,
                expectedClassName: String): (_DefaultParamsReader.Metadata, ClassifierType) = {

    val metadata = _DefaultParamsReader.loadMetadata(path, sc, expectedClassName)
    val classifierPath = new Path(path, "classifier").toString
    val estimator = _DefaultParamsReader.loadParamsInstance[ClassifierType](classifierPath, sc)
    (metadata, estimator)
  }
}

  /**
  * Model produced by [[OneVsRestMulti]].
  * This stores the models resulting from training k binary classifiers: one for each class.
  * Each example is scored against all k models, and all the models with confidence above
  * `threshold` are picked to label the example.
  *
  * @param models The binary classification models for the reduction.
  *               The i-th model is produced by testing the i-th class (taking label 1) vs the rest
  *               (taking label 0).
  */
final class OneVsRestMultiModel(
    override val uid: String,
    val labelsMapping: Map[Int, String],
    val models: Array[_ <: ClassificationModel[_, _]])
  extends Model[OneVsRestMultiModel]
    with OneVsRestMultiParams  with MLWritable {


    override def transformSchema(schema: StructType): StructType = {
//        validateAndTransformSchema(schema, fitting = false, getClassifier.featuresDataType)
      schema.add(StructField($(predictionCol), DoubleType, false))
    }

    override def transform(dataset: Dataset[_]): DataFrame = {
      // Check schema
      transformSchema(dataset.schema, logging = true)

      // determine the input columns: these need to be passed through
      val origCols = dataset.schema.map(f => col(f.name))

      // add an accumulator column to store predictions of all the models
      val accColName = "mbc$acc" + UUID.randomUUID().toString
      val initUDF = udf { () => Map[Int, Double]() }
      val newDataset = dataset.withColumn(accColName, initUDF())

      // persist if underlying dataset is not persistent.
      val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
      if (handlePersistence) {
        newDataset.persist(StorageLevel.MEMORY_AND_DISK)
      }

      // update the accumulator column with the result of prediction of models
      val aggregatedDataset = models.zipWithIndex.foldLeft[DataFrame](newDataset) {
        case (df, (model, index)) =>
          val predictionCol = model.getPredictionCol
          val featuresCol = model.getFeaturesCol

          val columns = origCols ++ List(col(predictionCol), col(accColName))

          // add temporary column to store intermediate scores and update
          val tmpColName = "mbc$tmp" + UUID.randomUUID().toString

          val updateUDF = udf { (predictions: Map[Int, Double], prediction: Double) =>
            predictions + ((index, prediction))
          }

          model.setFeaturesCol(featuresCol)
          val transformedDataset = model.transform(df).select(columns: _*)
          val updatedDataset = transformedDataset
            .withColumn(tmpColName, updateUDF(col(accColName), col(predictionCol)))
          val newColumns = origCols ++ List(col(tmpColName))

          // switch out the intermediate column with the accumulator column
          updatedDataset.select(newColumns: _*).withColumnRenamed(tmpColName, accColName)
      }

      if (handlePersistence) {
        newDataset.unpersist()
      }


      // output the index of all classifiers with positive prediction
      val labelUDF = udf { (predictions: Map[Int, Double]) =>
        predictions.collect { case pred if pred._2 == 1 => pred._1.toDouble }.toSeq
      }
      val stringLabelUDF = udf { doubleLabels: Seq[Double] =>
        doubleLabels.map(pred => labelsMapping(pred.toInt))
      }

      // output label as prediction
      // Refactored line due to method withColumn(name, col, metadata) being private
      if(getStringPredictionCol != "") {
        aggregatedDataset
          .withColumn(getPredictionCol, labelUDF(col(accColName)))
          .withColumn(getStringPredictionCol, stringLabelUDF(col(getPredictionCol)))
          .drop(accColName)
      } else {
        aggregatedDataset
          .withColumn(getPredictionCol, labelUDF(col(accColName)))
          .drop(accColName)
      }


    }

    override def copy(extra: ParamMap): OneVsRestMultiModel = {
      val copied = new OneVsRestMultiModel(
        uid, labelsMapping, models.map(_.copy(extra).asInstanceOf[ClassificationModel[_, _]]))
      copyValues(copied, extra).setParent(parent)
    }

    override def write: MLWriter = new OneVsRestMultiModel.OneVsRestMultiModelWriter(this)
}

object OneVsRestMultiModel extends MLReadable[OneVsRestMultiModel] {

  override def read: MLReader[OneVsRestMultiModel] = new OneVsRestModelReader

  override def load(path: String): OneVsRestMultiModel = super.load(path)

  /** [[MLWriter]] instance for [[OneVsRestMultiModel]] */
  class OneVsRestMultiModelWriter(instance: OneVsRestMultiModel) extends MLWriter {

    OneVsRestMultiParams.validateParams(instance)

    override protected def saveImpl(path: String): Unit = {
      implicit val formats = org.json4s.DefaultFormats
      val extraJson = ("numClasses" -> instance.models.length) ~ ("labelMappings" -> Serialization.write(instance.labelsMapping))
      OneVsRestMultiParams.saveImpl(path, instance, sc, Some(extraJson))
      instance.models.zipWithIndex.foreach { case (model: MLWritable, idx) =>
        val modelPath = new Path(path, s"model_$idx").toString
        model.save(modelPath)
      }
    }
  }

  private class OneVsRestModelReader extends MLReader[OneVsRestMultiModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[OneVsRestMultiModel].getName

    override def load(path: String): OneVsRestMultiModel = {
      implicit val format = DefaultFormats
      val (metadata, classifier) = OneVsRestMultiParams.loadImpl(path, sc, className)
//      val labelMetadata = Metadata.fromJson((metadata.metadata \ "labelMetadata").extract[String])
      val numClasses = (metadata.metadata \ "numClasses").extract[Int]
      val labelMappings =  parse((metadata.metadata \ "labelMappings").extract[String]).extract[Map[Int,String]]

      val models = Range(0, numClasses).toArray.map { idx =>
        val modelPath = new Path(path, s"model_$idx").toString
        _DefaultParamsReader.loadParamsInstance[ClassificationModel[_, _]](modelPath, sc)
      }
      val ovrModel = new OneVsRestMultiModel(metadata.uid, labelMappings, models)
      metadata.getAndSetParams(ovrModel)
      ovrModel.set("classifier", classifier)
      ovrModel
    }
  }
}

/**
  * Reduction of Multiclass Classification to Binary Classification.
  * Performs reduction using one against all strategy.
  * For a multiclass stackoverflow.classification with k classes, train k models (one per class).
  * Each example is scored against all k models and the model with highest score
  * is picked to label the example.
  */
final class OneVsRestMulti(
    override val uid: String)
  extends Estimator[OneVsRestMultiModel]
    with OneVsRestMultiParams  with MLWritable {

  def this() = this(Identifiable.randomUID("oneVsRest"))

  /** @group setParam */
  def setClassifier(value: Classifier[_, _, _]): this.type = {
    set(classifier, value.asInstanceOf[ClassifierType])
  }

  override def transformSchema(schema: StructType): StructType = {
    //TODO: Improve this
//    validateAndTransformSchema(schema, fitting = true, getClassifier.featuresDataType)
    schema.add(StructField($(predictionCol), DoubleType, false))
  }

  def getLabels(dataset: Dataset[_]): Array[String] = {
    import dataset.sparkSession.implicits._
    dataset.select(explode(dataset($(labelCol)))).distinct().as[String].collect()
  }

  def computeNumClasses(dataset: Dataset[_]): Int = getLabels(dataset).length

  override def fit(dataset: Dataset[_]): OneVsRestMultiModel = {
    transformSchema(dataset.schema)

    val multiclassLabeled = dataset.select($(labelCol), $(featuresCol))

    // persist if underlying dataset is not persistent.
    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) {
      multiclassLabeled.persist(StorageLevel.MEMORY_AND_DISK)
    }

    // create k columns, one for each binary classifier.
    val labels = getLabels(dataset)
    var labelsMapping =  Map[Int, String]()
    val models = labels.zipWithIndex.map { case (label, index) =>
      labelsMapping = labelsMapping + ((index, label))
      val labelColName = "mc2b$" + index
      // Refactored this line due to method withColumn(name, col, metadata) being private
      val trainingDataset = multiclassLabeled.withColumn(
        labelColName, when(array_contains(col($(labelCol)), label), 1.0).otherwise(0.0))
      val classifier = getClassifier
      val paramMap = new ParamMap()
      paramMap.put(classifier.labelCol -> labelColName)
      paramMap.put(classifier.featuresCol -> getFeaturesCol)
      paramMap.put(classifier.predictionCol -> getPredictionCol)
      classifier.fit(trainingDataset, paramMap)
    }.toArray[ClassificationModel[_, _]]

    if (handlePersistence) {
      multiclassLabeled.unpersist()
    }

    val model = new OneVsRestMultiModel(uid, labelsMapping, models).setParent(this)
    copyValues(model)
  }

  override def copy(extra: ParamMap): OneVsRestMulti = {
    val copied = defaultCopy(extra).asInstanceOf[OneVsRestMulti]
    if (isDefined(classifier)) {
      copied.setClassifier($(classifier).copy(extra))
    }
    copied
  }

  override def write: MLWriter = new OneVsRestMulti.OneVsRestMultiWriter(this)
}

object OneVsRestMulti extends MLReadable[OneVsRestMulti] {

  override def read: MLReader[OneVsRestMulti] = new OneVsRestMultiReader

  override def load(path: String): OneVsRestMulti = super.load(path)

  /** [[MLWriter]] instance for [[OneVsRestMulti]] */
  class OneVsRestMultiWriter(instance: OneVsRestMulti) extends MLWriter {

    OneVsRestMultiParams.validateParams(instance)

    override protected def saveImpl(path: String): Unit = {
      OneVsRestMultiParams.saveImpl(path, instance, sc)
    }
  }

  private class OneVsRestMultiReader extends MLReader[OneVsRestMulti] {

    /** Checked against metadata when loading model */
    private val className = classOf[OneVsRestMulti].getName

    override def load(path: String): OneVsRestMulti = {
      val (metadata, classifier) = OneVsRestMultiParams.loadImpl(path, sc, className)
      val ovr = new OneVsRestMulti(metadata.uid)
      metadata.getAndSetParams(ovr)
      ovr.setClassifier(classifier)
    }
  }
}
