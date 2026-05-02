package osspulse.ingestion

import osspulse.schema.HfModelsSchema
import org.apache.spark.sql.SparkSession
import java.io.{BufferedWriter, FileWriter}

object IngestHfModels {

  case class Config(
    input: String = "",
    rawOutput: String = "data/raw/huggingface_hub",
    sampleOutput: String = "output/samples/raw_hf_models_sample.csv",
    sampleRows: Int = 10,
    writePartitions: Int = 16,
    writeMode: String = "overwrite"
  )

  def parseArgs(args: Array[String]): Config = {
    var config = Config()
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--input"            => config = config.copy(input = args(i + 1)); i += 2
        case "--raw-output"       => config = config.copy(rawOutput = args(i + 1)); i += 2
        case "--sample-output"    => config = config.copy(sampleOutput = args(i + 1)); i += 2
        case "--sample-rows"      => config = config.copy(sampleRows = args(i + 1).toInt); i += 2
        case "--write-partitions" => config = config.copy(writePartitions = args(i + 1).toInt); i += 2
        case "--write-mode"       => config = config.copy(writeMode = args(i + 1)); i += 2
        case other                => println(s"Unknown argument: $other"); i += 1
      }
    }
    require(config.input.nonEmpty, "--input is required")
    config
  }

  def ensureParentDir(path: String): Unit = {
    val parent = new java.io.File(path).getParentFile
    if (parent != null && !parent.exists()) parent.mkdirs()
  }

  def writeSampleCsv(spark: SparkSession, df: org.apache.spark.sql.DataFrame,
                     outputPath: String, sampleRows: Int): Unit = {
    val sample = df
      .select("modelId", "author", "pipeline_tag", "library_name", "downloads", "likes", "lastModified")
      .limit(sampleRows)
      .collect()

    ensureParentDir(outputPath)
    val writer = new BufferedWriter(new FileWriter(outputPath))
    writer.write("modelId,author,pipeline_tag,library_name,downloads,likes,lastModified\n")
    sample.foreach { row =>
      val values = (0 until row.length).map(i => Option(row.get(i)).getOrElse("").toString)
      writer.write(values.mkString(",") + "\n")
    }
    writer.close()
  }

  def main(args: Array[String]): Unit = {
    val config = parseArgs(args)

    val spark = SparkSession.builder()
      .appName("HFModelsIngestion")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()

    val rawDf = spark.read.schema(HfModelsSchema.hfModelsSchema).json(config.input)

    println("=== Raw HF Models Schema ===")
    rawDf.printSchema()
    println(s"Total raw records: ${rawDf.count()}")

    rawDf.repartition(config.writePartitions)
      .write.mode(config.writeMode).parquet(config.rawOutput)

    writeSampleCsv(spark, rawDf, config.sampleOutput, config.sampleRows)

    spark.stop()
  }
}
