import time
import os


# The idea with this class, is if needed to monitor something else, then just the time
class SparkMonitor(object):
    def __init__(self):
        self.timer_ini = self.track_timer()
        self.time_to_read_ini = 0
        self.elapsed_read_time = 0
        self.time_to_write_ini = 0
        self.elapsed_write_time = 0
        self.timer_end = 0

    def get_formatted_file_size(self, file_path):
        file_size_bytes = os.path.getsize(file_path)
        return self.format_size(file_size_bytes)

    def format_size(self, size_in_bytes):
        units = ["B", "KB", "MB", "GB", "TB"]

        unit_index = 0
        size = size_in_bytes
        while size > 1024 and unit_index < len(units) - 1:
            size /= 1024.0
            unit_index += 1

        formatted_size = f"{size:.2f} {units[unit_index]}"
        return formatted_size

    def format_time(self, duration_seconds):
        if duration_seconds < 60:
            return f"{duration_seconds:.2f} sec"
        else:
            duration_minutes = duration_seconds / 60.0
            return f"{duration_minutes:.2f} min"

    def track_timer(self):
        return time.time()

    def start_read_timer(self):
        self.time_to_read_ini = self.track_timer()

    def start_write_timer(self):
        self.time_to_write_ini = self.track_timer()

    def track_read_time(self):
        self.elapsed_read_time = self.track_timer() - self.time_to_read_ini

    def track_write_time(self):
        self.elapsed_write_time = self.track_timer() - self.time_to_write_ini

    def write_results_to_file(self, in_file_name, out_file_name, in_format, out_format):
        total = self.track_timer() - self.timer_ini
        input_file_size = self.get_formatted_file_size(in_file_name)
        output_file_size = self.get_formatted_file_size(out_file_name)

        # Write the result to a file
        with open(f"metric_data.txt", "a") as file:
            file.write(
                f"\n{self.format_time(self.elapsed_read_time)},{self.format_time(self.elapsed_write_time)},{self.format_time(total)},{input_file_size},{output_file_size},{in_file_name},{out_file_name},{in_format},{out_format}"
            )

    # TODO - Implement a monitor for RAM and CPU
