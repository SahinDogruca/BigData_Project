#!/usr/bin/env python3
from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

class MaxSeverity(MRJob):
    """
    MapReduce job to find the maximum accident severity
    """
    
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
            
            # Severity sütununun indeksini belirle
            severity_idx = 2
            
            # Severity değerini al
            severity = int(row[severity_idx])
            
            # Severity değerini anahtar olarak döndür
            yield "max_severity", severity
        except Exception as e:
            yield "error", str(e)
    
    def reducer(self, key, values):
        if key == "max_severity":
            # Maximum değeri bul
            max_severity = max(values)
            yield key, max_severity
        else:
            # Hataları loglama
            for value in values:
                yield key, value

if __name__ == '__main__':
    MaxSeverity.run()

