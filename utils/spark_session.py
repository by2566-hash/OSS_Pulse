from __future__ import annotations

import os
from typing import Any

from pyspark.sql import SparkSession


def create_spark_session(
    app_name: str,
    shuffle_partitions: int = 200,
    extra_configs: dict[str, Any] | None = None,
) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "6g"))
        .config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "4g"))
        .config("spark.driver.maxResultSize", os.environ.get("SPARK_DRIVER_MAX_RESULT_SIZE", "1g"))
        .config("spark.sql.files.maxPartitionBytes", os.environ.get("SPARK_MAX_PARTITION_BYTES", "64m"))
        .config("spark.default.parallelism", os.environ.get("SPARK_DEFAULT_PARALLELISM", "16"))
        .config("spark.sql.adaptive.enabled", "true")
    )

    if extra_configs:
        for key, value in extra_configs.items():
            builder = builder.config(key, value)

    return builder.getOrCreate()
