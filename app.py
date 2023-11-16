import time
from spark_utils import spark_core
from spark_utils import spark_monitor
from spark_utils import utils


def main():
    [file_name, input_format, output_format] = utils.read_args_from_argv()

    monitor = spark_monitor.SparkMonitor()
    spark = spark_core.SparkCore("Saude", file_name, input_format, output_format)

    monitor.start_read_timer()
    df = spark.read_file_to_dataframe()
    monitor.track_read_time()

    query_df = spark.create_view_and_query(df)
    if output_format == "delta":
        query_df = spark.clean_columns_format(query_df)

    monitor.start_write_timer()
    spark.write_file_to_dataframe(query_df, file_name)
    monitor.track_write_time()

    spark.stop_spark()

    # TODO: If the user insert some name as a result, put a verification here
    output_dir = spark.data_result_dir
    print(output_dir)
    monitor.write_results_to_file(file_name, output_dir, input_format, output_format)


if __name__ == "__main__":
    main()
