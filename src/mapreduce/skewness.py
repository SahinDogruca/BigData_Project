#!/usr/bin/env python3
from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
import math

class SkewnessSeverity(MRJob):
    """
    MapReduce job to calculate skewness of accident severity distribution
    """
    
    def steps(self):
        return [
            # İlk adım: Ortalama ve standart sapmayı hesapla
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper_stats,
                   reducer=self.reducer_stats),
                   
            # İkinci adım: Çarpıklık (Skewness) hesapla
            MRStep(mapper=self.mapper_skewness,
                   reducer=self.reducer_skewness)
        ]
    
    def mapper_init(self):
        self.is_header = True
    
    def mapper_stats(self, _, line):
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
            
            # Değeri ilet
            yield "severity", severity
        except Exception as e:
            yield "error", str(e)
    
    def reducer_stats(self, key, values):
        if key == "severity":
            all_values = list(values)
            n = len(all_values)
            
            if n > 0:
                # Ortalama hesapla
                mean = sum(all_values) / n
                
                # Varyans hesapla
                variance = sum((x - mean) ** 2 for x in all_values) / n
                std_dev = math.sqrt(variance)
                
                # Tüm değerleri, ortalamayı ve std_dev'i bir sonraki adıma ilet
                for val in all_values:
                    yield None, (val, mean, std_dev, n)
    
    def mapper_skewness(self, _, value_stats):
        value, mean, std_dev, n = value_stats
        
        # Çarpıklık formülü için pay hesabı
        if std_dev > 0:
            z_score = (value - mean) / std_dev
            skewness_contribution = z_score ** 3
            yield "skewness", (skewness_contribution, 1, n)
        else:
            yield "skewness", (0, 1, n)
    
    def reducer_skewness(self, key, values):
        sum_skewness = 0
        count = 0
        n = None
        
        # Tüm katkıları topla
        for skew_contrib, cnt, sample_size in values:
            sum_skewness += skew_contrib
            count += cnt
            if n is None:
                n = sample_size
        
        # Çarpıklık hesapla
        if n > 0:
            skewness = sum_skewness / n
            
            yield "skewness_result", {
                "skewness": skewness,
                "sample_size": n,
                "interpretation": self.interpret_skewness(skewness)
            }
    
    def interpret_skewness(self, skewness):
        """Çarpıklık değerini yorumla"""
        if skewness > 0.5:
            return "Pozitif çarpıklık (Sağa eğilimli dağılım)"
        elif skewness < -0.5:
            return "Negatif çarpıklık (Sola eğilimli dağılım)"
        else:
            return "Yaklaşık simetrik dağılım"

if __name__ == '__main__':
    SkewnessSeverity.run()

