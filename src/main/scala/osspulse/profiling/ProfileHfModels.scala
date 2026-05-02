package osspulse.profiling

import osspulse.schema.HfModelsSchema
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.io.{BufferedWriter, FileWriter}

object ProfileHfModels {

  case class Config(
    input: String = "",
    inputFormat: String = "parquet",
    summaryOutput: String = "output/profiling/hf_models_profile_summary.csv",
    pipelineTagOutput: String = "output/profiling/hf_models_pipeline_tag_distribution.csv",
    libraryOutput: String = "output/profiling/hf_models_library_distribution.csv",
    nullOutput: String = "output/profiling/hf_models_null_counts.csv",
    topAuthorsOutput: String = "output/profiling/hf_models_top_authors.csv",
    topDownloadsOutput: String = "output/profiling/hf_models_top_downloads.csv",
    sampleOutput: String = "output/samples/raw_hf_models_profile_sample.csv",
    sampleRows: Int = 10
  )

  def parseArgs(args: Array[String]): Config = {
    var config = Config()
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--input"               => config = config.copy(input = args(i + 1)); i += 2
        case "--input-format"        => config = config.copy(inputFormat = args(i + 1)); i += 2
        case "--summary-output"      => config = config.copy(summaryOutput = args(i + 1)); i += 2
        case "--pipeline-tag-output" => config = config.copy(pipelineTagOutput = args(i + 1)); i += 2
        case "--library-output"      => config = config.copy(libraryOutput = args(i + 1)); i += 2
        case "--null-output"         => config = config.copy(nullOutput = args(i + 1)); i += 2
        case "--top-authors-output"  => config = config.copy(topAuthorsOutput = args(i + 1)); i += 2
        case "--top-downloads-output"=> config = config.copy(topDownloadsOutput = args(i + 1)); i += 2
        case "--sample-output"       => config = config.copy(sampleOutput = args(i + 1)); i += 2
        case "--sample-rows"         => config = config.copy(sampleRows = args(i + 1).toInt); i += 2
        case other                   => println(s"Unknown argument: $other"); i += 1
      }
    }
    require(config.input.nonEmpty, "--input is required")
    config
  }

  def ensureParentDir(path: String): Unit = {
    val parent = new java.io.File(path).getParentFile
    if (parent != null && !parent.exists()) parent.mkdirs()
  }

  def writeSingleCsv(rows: Seq[(String, Any)], outputPath: String): Unit = {
    ensureParentDir(outputPath)
    val writer = new BufferedWriter(new FileWriter(outputPath))
    writer.write("metric,value\n")
    rows.foreach { case (metric, value) =>
      writer.write(s"$metric,$value\n")
    }
    writer.close()
  }

  def writeDfAsCsv(df: DataFrame, outputPath: String): Unit = {
    val rows = df.collect()
    val header = df.columns.mkString(",")
    ensureParentDir(outputPath)
    val writer = new BufferedWriter(new FileWriter(outputPath))
    writer.write(header + "\n")
    rows.foreach { row =>
      val values = (0 until row.length).map(i => Option(row.get(i)).getOrElse("").toString)
      writer.write(values.mkString(",") + "\n")
    }
    writer.close()
  }

  def readInput(spark: SparkSession, path: String, format: String): DataFrame = {
    if (format == "json") spark.read.schema(HfModelsSchema.hfModelsSchema).json(path)
    else spark.read.parquet(path)
  }

  def main(args: Array[String]): Unit = {
    val config = parseArgs(args)

    val spark = SparkSession.builder()
      .appName("HFModelsProfiling")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()

    val df = readInput(spark, config.input, config.inputFormat)

    // --- summary metrics ---
    val totalRows = df.count()
    val totalColumns = df.columns.length

    val authorCol = if (df.columns.contains("author")) "author" else "owner"
    val distinctAuthors = df.select(authorCol).distinct().count()

    val pipelineTagCol = "pipeline_tag"
    val distinctPipelineTags = df.filter(col(pipelineTagCol).isNotNull).select(pipelineTagCol).distinct().count()

    val libraryCol = "library_name"
    val distinctLibraries = df.filter(col(libraryCol).isNotNull).select(libraryCol).distinct().count()

    val downloadsStats = df.agg(
      min("downloads").alias("min_downloads"),
      max("downloads").alias("max_downloads"),
      round(avg("downloads"), 2).alias("avg_downloads"),
      expr("percentile_approx(downloads, 0.5)").alias("median_downloads")
    ).first()

    val likesStats = df.agg(
      min("likes").alias("min_likes"),
      max("likes").alias("max_likes"),
      round(avg("likes"), 2).alias("avg_likes")
    ).first()

    val summaryRows = Seq(
      ("total_rows", totalRows),
      ("total_columns", totalColumns),
      ("distinct_authors", distinctAuthors),
      ("distinct_pipeline_tags", distinctPipelineTags),
      ("distinct_library_names", distinctLibraries),
      ("min_downloads", downloadsStats.getAs[Any]("min_downloads")),
      ("max_downloads", downloadsStats.getAs[Any]("max_downloads")),
      ("avg_downloads", downloadsStats.getAs[Any]("avg_downloads")),
      ("median_downloads", downloadsStats.getAs[Any]("median_downloads")),
      ("min_likes", likesStats.getAs[Any]("min_likes")),
      ("max_likes", likesStats.getAs[Any]("max_likes")),
      ("avg_likes", likesStats.getAs[Any]("avg_likes"))
    )
    writeSingleCsv(summaryRows, config.summaryOutput)

    // --- pipeline_tag distribution ---
    val pipelineTagDf = df
      .select(coalesce(col(pipelineTagCol), lit("(null)")).alias("pipeline_tag"))
      .groupBy("pipeline_tag").count()
      .orderBy(desc("count"))
    writeDfAsCsv(pipelineTagDf, config.pipelineTagOutput)

    // --- library_name distribution ---
    val libraryDf = df
      .select(coalesce(col(libraryCol), lit("(null)")).alias("library_name"))
      .groupBy("library_name").count()
      .orderBy(desc("count"))
    writeDfAsCsv(libraryDf, config.libraryOutput)

    // --- null counts ---
    val columnsToCheck = Seq(
      "modelId", "model_id", "author", "pipeline_tag", "library_name",
      "downloads", "likes", "lastModified", "last_modified_ts", "created_at", "created_ts"
    ).filter(df.columns.contains)

    val nullExprs = columnsToCheck.map { c =>
      sum(col(c).isNull.cast("int")).alias(s"${c}_nulls")
    }

    // license and datasets
    val licenseCol = if (df.columns.contains("card_data")) "card_data.license" else "license"
    val datasetsCol = if (df.columns.contains("card_data")) "card_data.datasets" else "training_datasets"
    val extraNulls = Seq(
      (licenseCol, "license_nulls"),
      (datasetsCol, "datasets_nulls")
    ).filter { case (c, _) => df.columns.contains(c.split("\\.")(0)) }
      .map { case (c, alias) => sum(col(c).isNull.cast("int")).alias(alias) }

    val allNullExprs = nullExprs ++ extraNulls
    val nullCounts = df.agg(allNullExprs.head, allNullExprs.tail: _*).first().getValuesMap[Any](allNullExprs.map {
      _.expr.asInstanceOf[org.apache.spark.sql.catalyst.expressions.Alias].name
    })
    writeSingleCsv(nullCounts.toSeq, config.nullOutput)

    // --- top 20 authors by model count ---
    val topAuthorsDf = df
      .filter(col(authorCol).isNotNull)
      .groupBy(authorCol).count()
      .orderBy(desc("count"))
      .limit(20)
    writeDfAsCsv(topAuthorsDf, config.topAuthorsOutput)

    // --- top 20 models by downloads ---
    val modelIdCol = if (df.columns.contains("modelId")) "modelId" else "model_id"
    val topDownloadsCols = Seq(modelIdCol, "author", "pipeline_tag", "library_name", "downloads", "likes")
      .filter(df.columns.contains)
    val topDownloadsDf = df
      .filter(col("downloads").isNotNull)
      .select(topDownloadsCols.map(col): _*)
      .orderBy(desc("downloads"))
      .limit(20)
    writeDfAsCsv(topDownloadsDf, config.topDownloadsOutput)

    // --- sample rows ---
    val tsCol = if (df.columns.contains("lastModified")) "lastModified" else "last_modified_ts"
    val sampleCols = Seq(modelIdCol, "author", "pipeline_tag", "library_name", "downloads", "likes", tsCol)
      .filter(df.columns.contains)
    val sampleDf = df.select(sampleCols.map(col): _*).limit(config.sampleRows)
    writeDfAsCsv(sampleDf, config.sampleOutput)

    spark.stop()
  }
}
