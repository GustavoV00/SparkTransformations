import re
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta import *
from delta.pip_utils import configure_spark_with_delta_pip


# Class is responsible to initialize, create the configurations and stop the the spark
class SparkCore(object):
    """
    Spark class that do some basic configuration
    """

    def __init__(
        self,
        app_name,
        file_name,
        input_format,
        output_format,
    ):
        self.spark = self.spark_config_with_or_no_delta(app_name)
        self.file_name = file_name
        self.input_format = input_format
        self.output_format = output_format
        self.options = self.default_options()
        self.data_result_dir = ""

    def stop_spark(self):
        self.spark.stop()

    def spark_config_with_or_no_delta(self, app_name):
        # return SparkSession.builder.appName(app_name).getOrCreate()

        # To when i need to test something with unittest
        builder = (
            SparkSession.builder.appName("MyApp").master("spark://172.31.29.127:7077")
            .config("spark.hadoop.fs.s3a.endpoint", "http://172.31.29.127:9000/")
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin" )
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            # .config("spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled", "true")
            # .config("spark.shuffle.service.enabled", "true")
            # .config("spark.dynamicAllocation.enabled", "true")
            # .config("spark.shuffle.service.enabled", "false")
            # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            # .config(
            #     "spark.sql.catalog.spark_catalog",
            #     "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            # )
            # .config("spark.driver.host", "172.31.29.127:7077")
            # .config("spark.dynamicAllocation.enabled", "false")
            # .config("spark.master.memory", "4g")
            # .config("spark.executor.memory", "4g")
        )

        return configure_spark_with_delta_pip(builder).getOrCreate()

    def read_file_to_dataframe(self):
        """
        A method that reads file from an input an save as dataframe
        """
        print(self.file_name)
        print(self.input_format)
        return (
            self.spark.read.format(self.input_format)
            .options(**self.options)
            .load(self.file_name)
        )

    def write_file_to_dataframe(self, query_df, file_name):
        """
        A method that writes take a dataframe and write it to a dir
        """
        self.data_result_dir = (
            f"s3a://ici/from_spark/{file_name}_{self.input_format}_to_{self.output_format}"
        )
        query_df.write.format(self.output_format).mode("overwrite").save(
            self.data_result_dir
        )

    def default_sql(self, table):
        """
        A default Query for sql
        """
        return f"SELECT * FROM {table}"

    # If needed to change the sql query
    def set_sql(self, sql):
        """
        If is needed a custom query
        """
        self.sql = sql

    # Need to change this, in a way that i can edit in command line (TODO - future)
    def default_options(self):
        """
        Default options to when spark reads the data
        """
        return {"inferSchema": "True", "header": "True"}

    # if needed to change the options
    def set_options(self, options):
        """
        IF is needed custom options
        """
        self.options = options

    def create_view_and_query(self, df):
        table = "saude"
        df.createOrReplaceTempView(table)
        return self.spark.sql(self.default_sql(table))

    def clean_columns_format(self, df):
        cleaned_columns = [re.sub(r"[ ,;{}()\n\t=]", "_", col) for col in df.columns]
        return df.toDF(*cleaned_columns)
