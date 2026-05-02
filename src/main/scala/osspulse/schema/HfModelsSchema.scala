package osspulse.schema

import org.apache.spark.sql.types._

object HfModelsSchema {

  val cardDataSchema: StructType = StructType(Seq(
    StructField("license", StringType, nullable = true),
    StructField("datasets", ArrayType(StringType), nullable = true),
    StructField("language", ArrayType(StringType), nullable = true),
    StructField("base_model", StringType, nullable = true)
  ))

  val safetensorsParametersSchema: StructType = StructType(Seq(
    StructField("total", LongType, nullable = true)
  ))

  val safetensorsSchema: StructType = StructType(Seq(
    StructField("parameters", safetensorsParametersSchema, nullable = true)
  ))

  val configSchema: StructType = StructType(Seq(
    StructField("model_type", StringType, nullable = true),
    StructField("architectures", ArrayType(StringType), nullable = true)
  ))

  val hfModelsSchema: StructType = StructType(Seq(
    StructField("modelId", StringType, nullable = true),
    StructField("author", StringType, nullable = true),
    StructField("sha", StringType, nullable = true),
    StructField("lastModified", StringType, nullable = true),
    StructField("private", BooleanType, nullable = true),
    StructField("disabled", BooleanType, nullable = true),
    StructField("gated", StringType, nullable = true),
    StructField("pipeline_tag", StringType, nullable = true),
    StructField("tags", ArrayType(StringType), nullable = true),
    StructField("library_name", StringType, nullable = true),
    StructField("likes", LongType, nullable = true),
    StructField("downloads", LongType, nullable = true),
    StructField("downloads_all_time", LongType, nullable = true),
    StructField("trending_score", LongType, nullable = true),
    StructField("card_data", cardDataSchema, nullable = true),
    StructField("safetensors", safetensorsSchema, nullable = true),
    StructField("config", configSchema, nullable = true),
    StructField("created_at", StringType, nullable = true)
  ))

  val hfEvalResultsSchema: StructType = StructType(Seq(
    StructField("model_id", StringType, nullable = true),
    StructField("task_type", StringType, nullable = true),
    StructField("dataset_type", StringType, nullable = true),
    StructField("dataset_name", StringType, nullable = true),
    StructField("dataset_config", StringType, nullable = true),
    StructField("dataset_split", StringType, nullable = true),
    StructField("metric_type", StringType, nullable = true),
    StructField("metric_value", StringType, nullable = true),
    StructField("verified", BooleanType, nullable = true)
  ))

  val corePipelineTags: Seq[String] = Seq(
    "text-generation", "text-classification", "token-classification",
    "question-answering", "fill-mask", "summarization", "translation",
    "text2text-generation", "image-classification", "object-detection",
    "image-segmentation", "text-to-image", "image-to-text",
    "automatic-speech-recognition", "text-to-speech", "feature-extraction",
    "sentence-similarity", "zero-shot-classification", "reinforcement-learning",
    "depth-estimation"
  )

  val licenseNormalizeMap: Map[String, String] = Map(
    "apache 2.0" -> "apache-2.0",
    "apache2" -> "apache-2.0",
    "apache-2" -> "apache-2.0",
    "mit license" -> "mit",
    "gpl-3" -> "gpl-3.0",
    "gpl3" -> "gpl-3.0",
    "gplv3" -> "gpl-3.0",
    "bsd-3" -> "bsd-3-clause",
    "bsd-2" -> "bsd-2-clause",
    "cc-by-4" -> "cc-by-4.0",
    "cc-by-sa-4" -> "cc-by-sa-4.0",
    "cc-by-nc-4" -> "cc-by-nc-4.0",
    "cc-by-nc-sa-4" -> "cc-by-nc-sa-4.0"
  )
}
