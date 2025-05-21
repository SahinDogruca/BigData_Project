#!/usr/bin/env python3
from mrjob.job import MRJob
from mrjob.step import MRStep
import csv


class MinMaxNormalization(MRJob):
    """
    MapReduce job to normalize numerical features using min-max normalization
    """

    def configure_args(self):
        super(MinMaxNormalization, self).configure_args()
        self.add_passthru_arg(
            "--column",
            type=int,
            default=9,
            help="Index of the numerical column to normalize (0-based)",
        )

    def steps(self):
        return [
            # First step: Find Min and Max values
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper_find_min_max,
                reducer=self.reducer_find_min_max,
            ),
            # Second step: Apply normalization
            MRStep(mapper=self.mapper_normalize, reducer=self.reducer_normalize),
        ]

    def mapper_init(self):
        self.is_header = True

    def mapper_find_min_max(self, _, line):
        # Skip header line
        if self.is_header:
            self.is_header = False
            return

        try:
            # Parse CSV line
            row = next(csv.reader([line]))

            column_idx = self.options.column

            # Get value from specified column
            value_str = row[column_idx]
            if value_str and value_str != "":
                value = float(value_str)

                # Emit for min/max calculation
                yield "value", value
        except Exception as e:
            yield "error", str(e)

    def reducer_find_min_max(self, key, values):
        if key == "value":
            # Get all values
            all_values = list(values)
            if all_values:
                min_val = min(all_values)
                max_val = max(all_values)

                # Emit each value with min and max for normalization
                for val in all_values:
                    yield None, (val, min_val, max_val)

    def mapper_normalize(self, _, value_min_max):
        value, min_val, max_val = value_min_max

        # Apply min-max normalization
        range_val = max_val - min_val
        if range_val > 0:
            normalized = (value - min_val) / range_val
        else:
            normalized = 0

        yield "normalized", (value, normalized)

    def reducer_normalize(self, key, values):
        # Output first 10 samples with original and normalized values
        count = 0
        for original, normalized in values:
            if count < 10:  # Show first 10 values as examples
                yield count, {"original": original, "normalized": normalized}
                count += 1


if __name__ == "__main__":
    MinMaxNormalization.run()

