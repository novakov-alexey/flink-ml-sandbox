package com.example

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
import org.apache.flinkx.api.*
import org.apache.flinkx.api.serializers.*
import org.slf4j.LoggerFactory

import java.io.File
import scala.jdk.CollectionConverters.*

import Common.*
import ExecutionMode.*
import java.util.Date

def getExecutionMode(args: String*) =
  if args.nonEmpty then
    if args(0) == "local" then LocalCluster
    else if args(0) == "app" then AppCluster
    else SessionCluster("localhost") // SessionCluster("sessioncluster-b98f04a6-a053-4570-8fdd-6fb426f640f9-jobmanager")
  else LocalCluster

@main def customerChurn(args: String*) =
  val logger = LoggerFactory.getLogger(this.getClass())
  val executionMode = getExecutionMode(args*)

  val (env, tEnv) = getEnv(executionMode)
  val exitedLabel = "Exited"

  val filePath =
    if args.length > 1 then Path(args(1))
    else if executionMode == AppCluster then Path("s3://vvp/artifacts/namespaces/default/Churn_Modelling.csv")
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
    .setParallelism(3) // it is tuned based on the current CSV data volume

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
  val categoricalCols = List("Geography", "Gender", "HasCrCard", "IsActiveMember")
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

  val stages = List[Stage[?]](
    indexer,
    geographyEncoder,
    assembler,
    standardScaler,
    finalAssembler,
    lr
  ).asJava

  val pipeline = Pipeline(stages)

  val validateSetSize = 2000
  val totalSetSize = 10000
  val trainSetSize = totalSetSize - validateSetSize
  val trainSet = trainData.limit(trainSetSize)
  val validateSet = trainData.limit(trainSetSize, validateSetSize)
  val pipelineModel = pipeline.fit(trainSet)

  // Test
  val validateResult = pipelineModel.transform(validateSet)(0)

  val accuracyCol = "accuracy"
  val correctLabelsCol = "correctLabelsCount"

  val evaluator = BinaryClassificationEvaluator()
    .setLabelCol(exitedLabel)
    .setMetricsNames(
      ClassifierMetric.AREA_UNDER_PR,
      ClassifierMetric.KS,
      ClassifierMetric.AREA_UNDER_ROC,
      ClassifierMetric.AREA_UNDER_LORENZ
    )

  lazy val currentDirectory = File(".").getCanonicalPath
  val catalogPath = if args.length > 2 then args(2) else s"file://$currentDirectory/target/catalog"
  val catalogName = if args.length > 3 then args(3) else "my_catalog"

  if !tEnv.listCatalogs().contains(catalogName) then tEnv.executeSql(s"""
        |CREATE CATALOG `$catalogName` WITH (
        |    'type'='paimon',
        |    'warehouse'='$catalogPath'
        |);""".stripMargin)

  val schema = Schema.newBuilder
    .column("executionTime", DataTypes.STRING().notNull())
    .column(correctLabelsCol, DataTypes.BIGINT())
    .column(accuracyCol, DataTypes.DOUBLE())
    .column("lastUpdate", DataTypes.TIMESTAMP(2))
    .primaryKey("executionTime")
    .build

  val tableExists = tEnv.listTables(catalogName, "default").contains("metricsSink")
  val sinkTableName = s"`$catalogName`.`default`.metricsSink"
  if !tableExists then
    tEnv.createTable(
      sinkTableName,
      TableDescriptor
        .forConnector("paimon")
        .schema(schema)
        .option("changelog-producer", "input")
        .build
    )

  val timeKey = Date().toString
  val stmtSet = tEnv.createStatementSet()
  val metricsQuery =
    s"""|insert into $sinkTableName
        |select
        |'$timeKey' as executionTime,         
        |count(*) as $correctLabelsCol,
        |count(*) / ${validateSetSize.toDouble} as $accuracyCol,
        |NOW() as lastUpdate
        |from $validateResult 
        |where $exitedLabel = $predictionCol
        |""".stripMargin
  stmtSet.addInsertSql(metricsQuery)

  // Uses the BinaryClassificationEvaluator object for evaluations.
  val outputTable = evaluator.transform(validateResult)(0)

  val evaluatorSchema = Schema.newBuilder
    .column(ClassifierMetric.AREA_UNDER_PR, DataTypes.DOUBLE())
    .column(ClassifierMetric.AREA_UNDER_ROC, DataTypes.DOUBLE())
    .column(ClassifierMetric.KS, DataTypes.DOUBLE())
    .column(ClassifierMetric.AREA_UNDER_LORENZ, DataTypes.DOUBLE())
    .column("executionTime", DataTypes.TIMESTAMP(2))
    .build

  val evaluatorTableName = s"`$catalogName`.`default`.evaluatorMetricsSink"
  if !tEnv.listTables(catalogName, "default").contains("evaluatorMetricsSink") then
    tEnv.createTable(
      evaluatorTableName,
      TableDescriptor
        .forConnector("paimon")
        .schema(evaluatorSchema)
        .build
    )

  val evaluatorMetricsQuery =
    s"""|insert into $evaluatorTableName
        |select 
        |${ClassifierMetric.AREA_UNDER_PR},
        |${ClassifierMetric.AREA_UNDER_ROC},
        |${ClassifierMetric.KS},
        |${ClassifierMetric.AREA_UNDER_LORENZ},
        |NOW() as executionTime
        |from $outputTable
        |""".stripMargin
  stmtSet.addInsertSql(evaluatorMetricsQuery)

  stmtSet.execute()
