from pyspark.sql import SparkSession
from pyspark.sql.functions import *


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
        output_dir,
    ):
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()
        self.file_name = file_name
        self.input_format = input_format
        self.output_format = output_format
        self.output_dir = output_dir
        self.options = self.default_options()

    def stop_spark(self):
        self.spark.stop()

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
        # return {"inferSchema": "True", "header": "True"}

    def write_file_to_dataframe(self, query_df):
        """
        A method that writes take a dataframe and write it to a dir
        """
        query_df.write.format(self.input_format).mode("overwrite").save(self.output_dir)

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
