import os
import fnmatch
import time
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, IntegerType, TimestampType, StructType, BooleanType



def get_json_paths():
    """
    Retrieves a list of paths for JSON files extracted from github

    Args:

    Returns:
        list: A list of paths of JSON files or raise a ValueError.
    """
    json_files = []
    for root, dirs, files in os.walk('../data'):
        for filename in fnmatch.filter(files, '*.json'):
            json_files.append(os.path.join(root, filename))
    if not json_files:
        raise ValueError("No JSON files found. Unable to proceed.")
    return json_files




if __name__ == '__main__':

    spark = SparkSession.builder.appName("Transformation").getOrCreate()

    schema = StructType(fields=[
        StructField("Organization Name", StringType(), nullable=False),
        StructField('repository_id', IntegerType(), nullable=False),
        StructField('repository_name', StringType(), nullable=False),
        StructField('repository_owner', StringType(), nullable=False),
        StructField("state", StringType(), nullable=False),
        StructField('merged_at', TimestampType(), nullable=False),
        StructField('merged', BooleanType(), nullable=True)
    ])

    # get JSON files paths
    json_files = get_json_paths()

    # read each JSON file individually and unionize dataframes
    dfs = [spark.read.schema(schema).json(path) for path in json_files]
    df = reduce(lambda df1, df2: df1.union(df2), dfs)

    # orginal structure
    # df.show()

    df.createOrReplaceTempView("my_table")

    # Perform necessary transformations using Spark SQL
    result_df = spark.sql("""
        SELECT
            "Organization Name", repository_id, repository_name, repository_owner,
            COUNT(*) AS num_prs,
            SUM(CASE WHEN merged THEN 1 ELSE 0 END) AS num_prs_merged,
            MAX(CASE WHEN merged THEN merged_at END) AS merged_at,
            (COUNT(*) = SUM(CASE WHEN merged THEN 1 ELSE 0 END) AND LOWER(repository_owner) LIKE '%scytale%') AS is_compliant
        FROM my_table
        GROUP BY "Organization Name", repository_id, repository_name, repository_owner
    """)

    # It can also be transformed using python
    # from pyspark.sql.functions import *
    # result_df = (df.groupBy("Organization Name", "repository_id", "repository_name", "repository_owner")
    #             .agg(
    #                 count("*").alias("num_prs"),
    #                 sum(when(col("merged"), 1).otherwise(0)).alias("num_prs_merged"),
    #                 max(when(col("merged"), col("merged_at"))).alias("merged_at"),
    #                 ((count("*") == sum(when(col("merged"), 1).otherwise(0))) & (lower(col("repository_owner")).contains("scytale"))).alias("is_compliant")
    #                 )
    #             )


    # Show transformed DataFrame
    result_df.show()

    # repartition the dataframe for distribution
    # result_df = result_df.repartition("Organization Name", "repository_id")

    # cache the dataframe if needed for multiple actions
    # result_df.cache()

    # Save it as a parquet file
    parquet_path = os.path.join("../data", "transformation.parquet")
    try:
        result_df.write.parquet(parquet_path)
    except:
        print(f"There is already a parquet file written in the specified path: {parquet_path}")

    # Stop the Spark session
    spark.stop()

    


    
