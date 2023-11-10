import time
from spark_utils import spark_core
from spark_utils import spark_monitor
from spark_utils import utils


def main():
    [file_name, input_format, output_format, output_dir] = utils.read_args_from_argv()
    print(file_name)
    monitor = spark_monitor.SparkMonitor()
    spark = spark_core.SparkCore(
        "Saude", file_name, input_format, output_format, output_dir
    )

    df = spark.read_file_to_dataframe()
    query_df = spark.create_view_and_query(df)
    spark.write_file_to_dataframe(query_df)

    spark.stop_spark()

    # TODO: If the user insert some name as a result, put a verification here
    result_file_text = f"{file_name}_{input_format}_to_{output_dir}.txt"
    monitor.write_results_to_file(result_file_text)


if __name__ == "__main__":
    main()
