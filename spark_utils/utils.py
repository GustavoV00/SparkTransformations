import sys


def read_args_from_argv():
    """
    Function to read command line arguments and return them in a list.

    Returns:
        list: A list containing the command line arguments.
    """

    file_name = sys.argv[1]
    input_format = sys.argv[2]
    output_format = sys.argv[3]

    return [file_name, input_format, output_format]
