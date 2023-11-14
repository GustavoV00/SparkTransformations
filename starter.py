import sys
import subprocess


def show_help():
    print("Usage: python starter.py [options]")
    print("Options:")
    print("  -h  Display help")
    print("  -u  Display usage example")
    print("  -r  Run the code")


def show_usage():
    print(
        "- This is an example script. It needs four args: data_file input_format output_format dir_result"
    )
    print(
        "- It's important to notice that, the app doesn't hold the / at the dir args, so don't need to put it"
    )
    print(
        "- The monitor results are stored at the results folder, with the following name: file_name_input_format_to_output_format_timestamp.txt"
    )
    print("  Example: python starter.py -r file_with_data.csv csv parquet saude")


def run_python_code(args):
    print("Running...")
    print(args)
    # subprocess.run(["spark-submit", "app.py"] + args)
    subprocess.run(
        [
            "spark-submit",
            "--packages",
            "io.delta:delta-core_2.12:2.4.0",
            "--packages",
            "io.delta:delta-spark_2.12:3.0.0",
            "--conf",
            "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
            "--conf",
            "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "app.py",
        ]
        + args
    )


# Parse command line options
if len(sys.argv) == 1:
    show_usage()
else:
    option = sys.argv[1]
    if option == "-h":
        show_help()
    elif option == "-u":
        show_usage()
    elif option == "-r":
        run_python_code(sys.argv[2:])
    else:
        print(f"Invalid option: {option}")
        show_help()
        sys.exit(1)
