from typing import List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, concat, row_number, desc, current_timestamp, upper
from delta.tables import DeltaTable


class transformations:

    def dedup(self, df: DataFrame, dedup_cols: List[str], cdc: str):
        df = df.withColumn("dedupKey", concat(*[col(c) for c in dedup_cols]))
        df = df.withColumn(
            "dedupCounts",
            row_number().over(Window.partitionBy("dedupKey").orderBy(desc(cdc)))
        )
        df = df.filter(col("dedupCounts") == 1)
        df = df.drop("dedupKey", "dedupCounts")
        return df

    def process_timestamp(self, df: DataFrame):
        df = df.withColumn("process_timestamp", current_timestamp())
        return df

    def upsert(self, spark: SparkSession, df: DataFrame, key_cols: List[str], table: str, cdc: str):
        # Build merge condition
        merge_condition = " AND ".join([f"src.{i} = trg.{i}" for i in key_cols])

        # Load existing Delta table
        dlt_obj = DeltaTable.forName(spark, f"pysparkdbt.silver.{table}")

        # Perform merge (UPSERT)
        dlt_obj.alias("trg").merge(
            df.alias("src"),
            merge_condition
        ).whenMatchedUpdateAll(
            condition=f"src.{cdc} >= trg.{cdc}"
        ).whenNotMatchedInsertAll().execute()

        return 1
