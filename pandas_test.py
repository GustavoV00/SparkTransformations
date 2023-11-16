import pandas as pd
import time
import sys


def format_time(duration_seconds):
    if duration_seconds < 60:
        return f"{duration_seconds:.4f} sec"
    else:
        minutes, seconds = divmod(duration_seconds, 60)
        return f"{minutes} min e {seconds:.2f} sec"


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Por favor, forneça o nome do arquivo CSV como argumento.")
        sys.exit(1)

    file_path = sys.argv[1]

    start_time = time.time()
    data = pd.read_csv(file_path)
    elapsed_time = time.time() - start_time

    print(f"Tempo para ler o arquivo CSV: {format_time(elapsed_time)}")
