package com.example

import Common.*
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.connector.file.src.reader.TextLineInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.ml.api.Stage
import org.apache.flink.ml.builder.Pipeline
import org.apache.flink.ml.classification.logisticregression.LogisticRegression
import org.apache.flink.ml.evaluation.binaryclassification.{
  BinaryClassificationEvaluator,
  BinaryClassificationEvaluatorParams as ClassifierMetric
}
import org.apache.flink.ml.feature.onehotencoder.OneHotEncoder
import org.apache.flink.ml.feature.standardscaler.StandardScaler
import org.apache.flink.ml.feature.stringindexer.{StringIndexer, StringIndexerParams}
import org.apache.flink.ml.feature.vectorassembler.VectorAssembler
import org.apache.flink.table.api.*
import org.apache.flink.types.Row

import org.apache.flinkx.api.conv.*
import org.apache.flinkx.api.serializers.*

import java.io.File
import scala.jdk.CollectionConverters.*

@main def customerChurn =
  // val hostname = args.headOption
  val hostname = Some("localhost") // Some("sessioncluster-b98f04a6-a053-4570-8fdd-6fb426f640f9-jobmanager")

  val (env, tEnv) = getEnv(hostname)
  val exitedLabel = "Exited"

  val filePath =
    if hostname.isDefined && !hostname.contains("localhost") then
      Path("s3://vvp/artifacts/namespaces/default/Churn_Modelling.csv")
    else Path.fromLocalFile(File(s"${File(".").getCanonicalPath}/data/Churn_Modelling.csv"))

  val source = FileSource
    .forRecordStreamFormat(
      TextLineInputFormat(),
      filePath
    )
    .build()
  val rawFeatureCols = Array(
    "CreditScore",
    "GeographyStr",
    "GenderStr",
    "Age",
    "Tenure",
    "Balance",
    "NumOfProducts",
    "HasCrCard",
    "IsActiveMember",
    "EstimatedSalary",
    exitedLabel
  )
  given rowTypes: RowTypeInfo = RowTypeInfo(
    doubleInfo +: (Array[TypeInformation[?]](stringInfo, stringInfo) ++ Array.fill(8)(doubleInfo)),
    rawFeatureCols
  )
  val skipColCount = 3
  val csvStream = env
    .fromSource(source, WatermarkStrategy.noWatermarks(), "trainingData")
    .filter(l => !l.startsWith("#"))
    .map(l =>
      // from `CreditScore` to `Exited` cols
      val row = l.split(",").slice(skipColCount, skipColCount + rawFeatureCols.length)
      Row.of(
        row(0).toDouble,
        row(1),
        row(2),
        row(3).toDouble,
        row(4).toDouble,
        row(5).toDouble,
        row(6).toDouble,
        row(7).toDouble,
        row(8).toDouble,
        row(9).toDouble,
        row(10).toDouble
      )
    )
    .setParallelism(3) // it is tunned based on the current CSV data volume

  val trainData = tEnv.fromDataStream(csvStream)

  // 1 - index Geography and Gender
  val indexer = StringIndexer()
    .setStringOrderType(StringIndexerParams.ALPHABET_ASC_ORDER)
    .setInputCols("GeographyStr", "GenderStr")
    .setOutputCols("GeographyInd", "GenderInd")

  // 2 - OneHot Encode Geography and Gender
  val geographyEncoder =
    OneHotEncoder()
      .setInputCols("GeographyInd", "GenderInd")
      .setOutputCols("Geography", "Gender")
      .setDropLast(false)

  // 3 - Merge to Vector
  val continuesCols = List(
    "CreditScore",
    "Age",
    "Tenure",
    "Balance",
    "NumOfProducts",
    "EstimatedSalary"
  )

  val assembler = VectorAssembler()
    .setInputCols(continuesCols*)
    .setOutputCol("continues_features")
    .setInputSizes(List.fill(continuesCols.length)(1).map(Integer.valueOf)*)

  // 4 - Normalize numbers
  val standardScaler =
    StandardScaler()
      .setWithMean(true)
      .setInputCol("continues_features")
      .setOutputCol("continues_features_s")

  // 5 - merge columns to features column
  val categoricalCols =
    List("Geography", "Gender", "HasCrCard", "IsActiveMember")
  val finalCols = categoricalCols :+ "continues_features_s"
  // Geography is 3 countries, Gender is 2 + other features
  val encodedFeatures = List(3, 2)
  val vectorSizes =
    encodedFeatures ++ List.fill(categoricalCols.length - encodedFeatures.length)(1) :+ continuesCols.length
  val finalAssembler = VectorAssembler()
    .setInputCols(finalCols*)
    .setOutputCol(featuresCol)
    .setInputSizes(vectorSizes.map(Integer.valueOf)*)

  // 6 - Train
  val lr = LogisticRegression()
    .setLearningRate(0.002d)
    .setLabelCol(exitedLabel)
    .setReg(0.1)
    .setElasticNet(0.5)
    .setMaxIter(100)
    .setTol(0.01d)
    .setGlobalBatchSize(64)

  val stages = (List[Stage[?]](
    indexer,
    geographyEncoder,
    assembler,
    standardScaler,
    finalAssembler,
    lr
  )).asJava

  val pipeline = Pipeline(stages)

  val testSetSize = 2000
  val totalSetSize = 10000
  val trainSetSize = totalSetSize - testSetSize
  val trainSet = trainData.limit(trainSetSize)
  val testSet = trainData.limit(trainSetSize, testSetSize)

  val pipelineModel = pipeline.fit(trainSet)

  // Test
  val testResult = pipelineModel.transform(testSet)(0)

  val resQuery =
    s"""|select 
        |$featuresCol, 
        |$exitedLabel as $labelCol, 
        |$predictionCol, 
        |rawPrediction        
        |from $testResult""".stripMargin
  val res = tEnv.sqlQuery(resQuery).execute
  val iter = res.collect

  val firstRow = iter.next
  val colNames = firstRow.getFieldNames(true).asScala.toList.mkString(", ")

  val correctCnt = (List(firstRow).toIterable ++ iter.asScala).foldLeft(0) { (acc, row) =>
    println(row)
    val label = row.getFieldAs[Double](labelCol)
    val prediction = row.getFieldAs[Double](predictionCol)
    if label == prediction then acc + 1 else acc
  }
  println(colNames)
  println(
    s"correct labels count: $correctCnt, accuracy: ${correctCnt / testSetSize.toDouble}"
  )

  val evaluator = BinaryClassificationEvaluator()
    .setLabelCol(exitedLabel)
    .setMetricsNames(
      ClassifierMetric.AREA_UNDER_PR,
      ClassifierMetric.KS,
      ClassifierMetric.AREA_UNDER_ROC,
      ClassifierMetric.AREA_UNDER_LORENZ
    )

  // Uses the BinaryClassificationEvaluator object for evaluations.
  val outputTable = evaluator.transform(testResult)(0)
  val evaluationResult = outputTable.execute.collect.next
  println(
    s"Area under the precision-recall curve: ${evaluationResult.getField(ClassifierMetric.AREA_UNDER_PR)}"
  )
  println(
    s"Area under the receiver operating characteristic curve: ${evaluationResult
        .getField(ClassifierMetric.AREA_UNDER_ROC)}"
  )
  println(
    s"Kolmogorov-Smirnov value: ${evaluationResult.getField(ClassifierMetric.KS)}"
  )
  println(
    s"Area under Lorenz curve: ${evaluationResult.getField(ClassifierMetric.AREA_UNDER_LORENZ)}"
  )
