from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)


card_data_schema = StructType(
    [
        StructField("license", StringType(), True),
        StructField("datasets", ArrayType(StringType()), True),
        StructField("language", ArrayType(StringType()), True),
        StructField("base_model", StringType(), True),
    ]
)

safetensors_parameters_schema = StructType(
    [
        StructField("total", LongType(), True),
    ]
)

safetensors_schema = StructType(
    [
        StructField("parameters", safetensors_parameters_schema, True),
    ]
)

hf_models_schema = StructType(
    [
        StructField("modelId", StringType(), True),
        StructField("author", StringType(), True),
        StructField("sha", StringType(), True),
        StructField("lastModified", StringType(), True),
        StructField("private", BooleanType(), True),
        StructField("disabled", BooleanType(), True),
        StructField("gated", StringType(), True),
        StructField("pipeline_tag", StringType(), True),
        StructField("tags", ArrayType(StringType()), True),
        StructField("library_name", StringType(), True),
        StructField("likes", LongType(), True),
        StructField("downloads", LongType(), True),
        StructField("downloads_all_time", LongType(), True),
        StructField("trending_score", LongType(), True),
        StructField("card_data", card_data_schema, True),
        StructField("safetensors", safetensors_schema, True),
        StructField("created_at", StringType(), True),
    ]
)

CORE_PIPELINE_TAGS = [
    "text-generation",
    "text-classification",
    "token-classification",
    "question-answering",
    "fill-mask",
    "summarization",
    "translation",
    "text2text-generation",
    "image-classification",
    "object-detection",
    "image-segmentation",
    "text-to-image",
    "image-to-text",
    "automatic-speech-recognition",
    "text-to-speech",
    "feature-extraction",
    "sentence-similarity",
    "zero-shot-classification",
    "reinforcement-learning",
    "depth-estimation",
]
