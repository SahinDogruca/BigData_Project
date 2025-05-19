#!/usr/bin/env python3
from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

class MinMaxNormalization(MRJob):
    """
    MapReduce job to normalize numerical features (e.g., Temperature)
    using min-max normalization
    """
    
    def steps(self):
        return [
            # İlk adım: Min ve Max değerlerini bul
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper_find_min_max,
                   reducer=self.reducer_find_min_max),
                   
            # İkinci adım: Normalizasyon uygula
            MRStep(mapper=self.mapper_normalize,
                   reducer=self.reducer_normalize)
        ]
    
    def mapper_init(self):
        self.is_header = True
    
    def mapper_find_min_max(self, _, line):
        # Başlık satırını atla
        if self.is_header:
            self.is_header = False
            return
            
        try:
            # CSV satırını parse et
            row = next(csv.reader([line]))
            
            # Sıcaklık sütununun indeksini belirle (örneğin - veri setine göre ayarlanmalı)
            # Kaggle US Accidents veri setinde Temperature(F) sütunu olabilir
            temperature_idx = 9  # Örneğin - doğru indeksi kontrol edin
            
            # Değeri al
            temp_str = row[temperature_idx]
            if temp_str and temp_str != "":
                temperature = float(temp_str)
                
                # Min ve max bulma için aynı anahtarla döndür
                yield "temperature", temperature
        except Exception as e:
            yield "error", str(e)
    
    def reducer_find_min_max(self, key, values):
        if key == "temperature":
            # Listenin tamamını al
            all_values = list(values)
            if all_values:
                min_val = min(all_values)
                max_val = max(all_values)
                
                # Min, max ve tüm değerleri bir sonraki adıma ilet
                for val in all_values:
                    yield None, (val, min_val, max_val)
    
    def mapper_normalize(self, _, value_min_max):
        value, min_val, max_val = value_min_max
        
        # Min-max normalizasyonu uygula
        range_val = max_val - min_val
        if range_val > 0:
            normalized = (value - min_val) / range_val
        else:
            normalized = 0
            
        yield "normalized", (value, normalized)
    
    def reducer_normalize(self, key, values):
        # İlk 10 örnek değeri ve onların normalize edilmiş değerlerini döndür
        count = 0
        for original, normalized in values:
            if count < 10:  # Sadece örnek olarak ilk 10 değeri göster
                yield count, {
                    "original": original,
                    "normalized": normalized
                }
                count += 1

if __name__ == '__main__':
    MinMaxNormalization.run()

