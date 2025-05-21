#!/usr/bin/env python3
from mrjob.job import MRJob
from mrjob.step import MRStep
import csv


class MeanValue(MRJob):
    """
    MapReduce job to calculate the average accident severity
    """

    def configure_args(self):
        super(MeanValue, self).configure_args()
        self.add_passthru_arg(
            "--column", type=int, default=2, help="Index of the column (0-based)"
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

            # (1, severity) çifti döndür (1 sayısı sayım için, severity toplam için)
            yield "value", (1, value)
        except Exception as e:
            # Hatalı satırlar için loglama yapabilirsiniz
            yield "error", str(e)

    def reducer(self, key, values):
        if key == "value":
            total_count = 0
            total_severity = 0

            # Tüm değerleri topla
            for count, severity in values:
                total_count += count
                total_severity += severity

            # Ortalamayı hesapla
            if total_count > 0:
                mean_value = total_severity / total_count
                yield "mean_value", mean_value
        else:
            # Hataları loglama
            for value in values:
                yield key, value


if __name__ == "__main__":
    MeanValue.run()
