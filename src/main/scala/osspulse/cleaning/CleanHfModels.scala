package osspulse.cleaning

import osspulse.schema.HfModelsSchema
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.io.{BufferedWriter, FileWriter}

object CleanHfModels {

  case class Config(
    input: String = "",
    inputFormat: String = "parquet",
    output: String = "data/cleaned/huggingface_hub",
    sampleOutput: String = "output/samples/clean_hf_models_sample.csv",
    sampleRows: Int = 10,
    writeMode: String = "overwrite"
  )

  def parseArgs(args: Array[String]): Config = {
    var config = Config()
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--input"         => config = config.copy(input = args(i + 1)); i += 2
        case "--input-format"  => config = config.copy(inputFormat = args(i + 1)); i += 2
        case "--output"        => config = config.copy(output = args(i + 1)); i += 2
        case "--sample-output" => config = config.copy(sampleOutput = args(i + 1)); i += 2
        case "--sample-rows"   => config = config.copy(sampleRows = args(i + 1).toInt); i += 2
        case "--write-mode"    => config = config.copy(writeMode = args(i + 1)); i += 2
        case other             => println(s"Unknown argument: $other"); i += 1
      }
    }
    require(config.input.nonEmpty, "--input is required")
    config
  }

  def ensureParentDir(path: String): Unit = {
    val parent = new java.io.File(path).getParentFile
    if (parent != null && !parent.exists()) parent.mkdirs()
  }

  def readInput(spark: SparkSession, path: String, format: String): DataFrame = {
    if (format == "json") spark.read.schema(HfModelsSchema.hfModelsSchema).json(path)
    else spark.read.parquet(path)
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

  def main(args: Array[String]): Unit = {
    val config = parseArgs(args)

    val spark = SparkSession.builder()
      .appName("HFModelsCleaning")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()

    import spark.implicits._

    val rawDf = readInput(spark, config.input, config.inputFormat)

    // 1. Filter private and disabled models
    val filteredDf = rawDf
      .filter(col("private") === false || col("private").isNull)
      .filter(col("disabled") === false || col("disabled").isNull)

    // 2. Select and transform columns
    var cleanedDf = filteredDf.select(
      col("modelId").alias("model_id"),
      split(col("modelId"), "/").getItem(0).alias("owner"),
      when(size(split(col("modelId"), "/")) > 1,
        split(col("modelId"), "/").getItem(1)
      ).alias("model_name"),
      lower(col("author")).alias("author"),
      to_timestamp(col("lastModified")).alias("last_modified_ts"),
      to_date(to_timestamp(col("lastModified"))).alias("last_modified_date"),
      to_timestamp(col("created_at")).alias("created_ts"),
      coalesce(lower(col("pipeline_tag")), lit("unknown")).alias("pipeline_tag"),
      coalesce(lower(col("library_name")), lit("unknown")).alias("library_name"),
      col("downloads"),
      col("likes"),
      col("downloads_all_time"),
      col("trending_score"),
      col("tags"),
      size(coalesce(col("tags"), array())).alias("tag_count"),
      lower(col("card_data.license")).alias("raw_license"),
      col("card_data.datasets").alias("training_datasets"),
      col("card_data.language").alias("languages"),
      col("card_data.base_model").alias("base_model"),
      col("safetensors.parameters.total").alias("parameter_count"),
      lower(col("config.model_type")).alias("model_type"),
      col("config.architectures").alias("architectures"),
      col("gated")
    )

    // 3. Filter bad data
    cleanedDf = cleanedDf
      .filter(col("model_id").isNotNull)
      .filter(col("model_id").contains("/"))
      .filter(col("last_modified_ts").isNotNull)
      .dropDuplicates(Seq("model_id"))

    // 4. Normalize license via mapping
    val licenseMapDf = spark.createDataFrame(
      HfModelsSchema.licenseNormalizeMap.toSeq
    ).toDF("lookup_license", "normalized_license")

    cleanedDf = cleanedDf
      .join(broadcast(licenseMapDf), cleanedDf("raw_license") === licenseMapDf("lookup_license"), "left")
      .withColumn("license",
        coalesce(col("normalized_license"), col("raw_license"), lit("unknown"))
      )
      .drop("raw_license", "normalized_license", "lookup_license")

    // 5. Write cleaned Parquet
    cleanedDf.write.mode(config.writeMode).parquet(config.output)

    // 6. Sample
    val sampleDf = cleanedDf
      .select("model_id", "owner", "model_name", "author", "pipeline_tag",
        "library_name", "license", "downloads", "likes", "parameter_count",
        "last_modified_ts")
      .orderBy(desc("downloads"))
      .limit(config.sampleRows)
    writeDfAsCsv(sampleDf, config.sampleOutput)

    println("=== Cleaned HF Models Schema ===")
    cleanedDf.printSchema()
    println(s"Total cleaned records: ${cleanedDf.count()}")

    spark.stop()
  }
}
