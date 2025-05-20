#!/usr/bin/env python3
from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
import math

class SkewnessSeverity(MRJob):
    """
    MapReduce job to calculate skewness of numerical column distribution
    """
    
    def configure_args(self):
        super(SkewnessSeverity, self).configure_args()
        self.add_passthru_arg(
            '--column', 
            type=int, 
            default=2,
            help='Index of the numerical column to analyze (0-based)'
        )
    
    def steps(self):
        return [
            # First step: Calculate mean and standard deviation
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper_stats,
                   reducer=self.reducer_stats),
                   
            # Second step: Calculate skewness
            MRStep(mapper=self.mapper_skewness,
                   reducer=self.reducer_skewness)
        ]
    
    def mapper_init(self):
        self.is_header = True
    
    def mapper_stats(self, _, line):
        # Skip header line
        if self.is_header:
            self.is_header = False
            return
            
        try:
            # Parse CSV line
            row = next(csv.reader([line]))
            
            # Get value from specified column
            column_idx = self.options.column
            value = float(row[column_idx])
            
            # Emit value
            yield "value", value
        except Exception as e:
            yield "error", str(e)
    
    def reducer_stats(self, key, values):
        if key == "value":
            all_values = list(values)
            n = len(all_values)
            
            if n > 0:
                # Calculate mean
                mean = sum(all_values) / n
                
                # Calculate variance and standard deviation
                variance = sum((x - mean) ** 2 for x in all_values) / n
                std_dev = math.sqrt(variance)
                
                # Emit all values with mean and std_dev for next step
                for val in all_values:
                    yield None, (val, mean, std_dev, n)
    
    def mapper_skewness(self, _, value_stats):
        value, mean, std_dev, n = value_stats
        
        # Calculate contribution to skewness
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
        
        # Sum all contributions
        for skew_contrib, cnt, sample_size in values:
            sum_skewness += skew_contrib
            count += cnt
            if n is None:
                n = sample_size
        
        # Calculate final skewness
        if n > 0:
            skewness = sum_skewness / n
            
            yield "skewness_result", {
                "skewness": skewness,
                "sample_size": n,
                "interpretation": self.interpret_skewness(skewness)
            }
    
    def interpret_skewness(self, skewness):
        """Interpret the skewness value"""
        if skewness > 0.5:
            return "Positive skew (Right-tailed distribution)"
        elif skewness < -0.5:
            return "Negative skew (Left-tailed distribution)"
        else:
            return "Approximately symmetric distribution"

if __name__ == '__main__':
    SkewnessSeverity.run()