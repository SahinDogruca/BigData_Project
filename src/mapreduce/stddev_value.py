#!/usr/bin/env python3
from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
import math


class StdDevValue(MRJob):
    """
    MapReduce job to calculate standard deviation of a numerical column
    (implements two-pass algorithm for std dev calculation)
    """

    def configure_args(self):
        super(StdDevValue, self).configure_args()
        self.add_passthru_arg(
            "--column",
            type=int,
            default=2,
            help="Index of the numerical column to analyze (0-based)",
        )

    def steps(self):
        return [
            # First step: Calculate mean
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper_mean,
                reducer=self.reducer_mean,
            ),
            # Second step: Calculate variance and standard deviation
            MRStep(mapper=self.mapper_variance, reducer=self.reducer_variance),
        ]

    def mapper_init(self):
        self.is_header = True

    def mapper_mean(self, _, line):
        # Skip header line
        if self.is_header:
            self.is_header = False
            return

        try:
            # Parse CSV line
            row = next(csv.reader([line]))

            column_idx = self.options.column
            value = float(row[column_idx])

            # Emit count and value
            yield "value", (1, value)
        except Exception as e:
            yield "error", str(e)

    def reducer_mean(self, key, values):
        if key == "value":
            count = 0
            total = 0
            all_values = []

            # Aggregate all values
            for cnt, val in values:
                count += cnt
                total += val
                all_values.append(val)

            # Calculate mean
            if count > 0:
                mean = total / count
                # Emit all values with mean and count for next step
                for val in all_values:
                    yield None, (val, mean, count)

    def mapper_variance(self, _, value_mean_count):
        value, mean, count = value_mean_count
        # Calculate squared difference for each value
        squared_diff = (value - mean) ** 2
        yield "variance", (squared_diff, 1, mean, count)

    def reducer_variance(self, key, values):
        sum_squared_diff = 0
        total_count = 0
        mean = None
        count = None

        # Sum all squared differences
        for squared_diff, cnt, m, c in values:
            sum_squared_diff += squared_diff
            total_count += cnt
            # All should have the same mean and count
            if mean is None:
                mean = m
                count = c

        # Calculate variance and standard deviation
        if total_count > 0:
            variance = sum_squared_diff / count
            std_dev = math.sqrt(variance)

            yield "statistics", {"mean": mean, "std_dev": std_dev, "count": count}


if __name__ == "__main__":
    StdDevValue.run()

