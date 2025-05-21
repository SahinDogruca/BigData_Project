#!/usr/bin/env python3
from mrjob.job import MRJob
from mrjob.step import MRStep
import csv


class MaxValue(MRJob):
    """
    MapReduce job to find the maximum accident severity
    """

    def configure_args(self):
        super(MaxValue, self).configure_args()
        self.add_passthru_arg(
            "--column",
            type=int,
            default=2,
            help="Index of the severity column (0-based)",
        )

    def mapper_init(self):
        # CSV başlıklarını atla
        self.is_header = True

    def mapper(self, _, line):
        # Başlık satırını atla
        if self.is_header:
            self.is_header = False
            return

        try:
            # CSV satırını parse et
            row = next(csv.reader([line]))

            idx = self.options.column
            value = int(row[idx])

            yield "max_value", value
        except Exception as e:
            yield "error", str(e)

    def reducer(self, key, values):
        if key == "max_value":
            # Maximum değeri bul
            max_value = max(values)
            yield key, max_value
        else:
            # Hataları loglama
            for value in values:
                yield key, value


if __name__ == "__main__":
    MaxValue.run()
