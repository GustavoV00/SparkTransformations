import time


# The idea with this class, is if needed to monitor something else, then just the time
class SparkMonitor(object):
    def __init__(self):
        self.start = self.track_timer()
        self.end = 0

    def track_timer(self):
        return time.time()

    def write_results_to_file(self, output_file):
        self.end = self.track_timer()
        result = self.end - self.start

        # Write the result to a file
        with open(f"results/{output_file}", "w") as file:
            file.write(f"Elapsed Time: {result} seconds")
