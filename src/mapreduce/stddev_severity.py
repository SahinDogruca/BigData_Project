#!/usr/bin/env python3
from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
import math

class StdDevSeverity(MRJob):
    """
    MapReduce job to calculate standard deviation of accident severity
    (implements two-pass algorithm for std dev calculation)
    """
    
    def steps(self):
        return [
            # İlk adım: Ortalamayı hesapla
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper_mean,
                   reducer=self.reducer_mean),
                   
            # İkinci adım: Varyansı hesapla
            MRStep(mapper=self.mapper_variance,
                   reducer=self.reducer_variance)
        ]
    
    def mapper_init(self):
        self.is_header = True
    
    def mapper_mean(self, _, line):
        # Başlık satırını atla
        if self.is_header:
            self.is_header = False
            return
            
        try:
            # CSV satırını parse et
            row = next(csv.reader([line]))
            
            # Severity değerini al
            severity_idx = 2
            severity = int(row[severity_idx])
            
            # Hem sayım hem de değer için çıktı ver
            yield "severity", (1, severity)
        except Exception as e:
            yield "error", str(e)
    
    def reducer_mean(self, key, values):
        if key == "severity":
            count = 0
            total = 0
            all_values = []
            
            # Tüm değerleri topla
            for cnt, val in values:
                count += cnt
                total += val
                all_values.append(val)
            
            # Ortalamayı hesapla
            if count > 0:
                mean = total / count
                # Tüm değerleri ve ortalamayı bir sonraki adıma ilet
                for val in all_values:
                    yield None, (val, mean, count)
    
    def mapper_variance(self, _, value_mean_count):
        value, mean, count = value_mean_count
        # Her değer için kareler farkını hesapla
        squared_diff = (value - mean) ** 2
        yield "variance", (squared_diff, 1, mean, count)
    
    def reducer_variance(self, key, values):
        sum_squared_diff = 0
        total_count = 0
        mean = None
        count = None
        
        # Tüm kare farklarını topla
        for squared_diff, cnt, m, c in values:
            sum_squared_diff += squared_diff
            total_count += cnt
            # Hepsinin aynı ortalama ve sayıma sahip olması gerekiyor
            if mean is None:
                mean = m
                count = c
        
        # Varyansı hesapla
        if total_count > 0:
            variance = sum_squared_diff / count
            std_dev = math.sqrt(variance)
            
            yield "statistics", {
                "mean": mean,
                "std_dev": std_dev,
                "count": count
            }

if __name__ == '__main__':
    StdDevSeverity.run()

