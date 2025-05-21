# Distributed Statistical Analysis of US Traffic Accidents

## Project Overview

This project implements a distributed analytical system for processing large-scale traffic accident data using Hadoop MapReduce. The system analyzes over 4 million accident records from the US Accidents dataset (2016-2023) to compute statistical functions including mean, maximum, standard deviation, min-max normalization, and skewness of accident severity.

## System Requirements

```
OS: Linux Ubuntu 22.04
Hadoop Version: 3.2.4
Java Version: 11
Python Version: 3.12
```

## Dataset

- **Source**: US Accidents (2016-2023) from Kaggle
- **Size**: 4+ million records, ~3.2 GB
- **Attributes**: 46 features including location, severity, weather conditions, timestamps
- **Download**: <https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents>

## Available Statistical Functions

1. **Mean** - Analyze average accident severity
2. **Maximum** - Identify highest accident severity level
3. **Standard Deviation** - Measure variability in accident severity
4. **Min-Max Normalization** - Normalize numerical features (temperature, visibility)
5. **Skewness** - Detect asymmetry in accident severity distribution

## Setup Instructions

### Prerequisites

- Hadoop environment correctly installed and configured
- US Accidents dataset downloaded from Kaggle
- Python PyQt5 installed for GUI interface

### Installation Steps

1. **Start Hadoop Services**

   ```bash
   start-dfs.sh
   start-yarn.sh
   ```

2. **Create Required HDFS Directories**

   ```bash
   hdfs dfs -mkdir -p /user/student/us-accidents/data
   hdfs dfs -mkdir -p /user/student/us-accidents/outputs
   ```

3. **Upload Dataset to HDFS**

   - Use the GUI interface to upload your US Accidents CSV file
   - Navigate to the data upload section in the application
   - Select your dataset file and confirm upload to HDFS

4. **Launch the Application**

   ```bash
   python3 gui.py
   ```

## Usage Instructions

1. **Select Dataset**: Choose the uploaded US Accidents dataset from the available files
2. **Choose Analysis Method**: Select from the 5 available statistical functions:
   - Mean calculation
   - Maximum value identification
   - Standard deviation computation
   - Min-max normalization
   - Skewness analysis
3. **Select Column**: Choose the target column for analysis (e.g., Severity, Temperature, Visibility)
4. **Execute Analysis**: Click "Run Analysis" and wait approximately 2-3 minutes for MapReduce job completion
5. **View Results**: Results will be displayed in the GUI with statistical summaries and visualizations

## Performance Expectations

- **Small datasets (100K records)**: ~45 seconds
- **Medium datasets (1M records)**: ~3.2 minutes
- **Full dataset (4M records)**: ~5 minutes
- **Performance improvement**: 3.5x faster than single-machine processing

## System Architecture

- **Storage Layer**: HDFS for distributed data storage
- **Processing Layer**: Hadoop MapReduce for parallel statistical computations
- **Presentation Layer**: Python PyQt5 GUI for user interaction and results visualization

## Troubleshooting

### Common Issues

1. **"Permission denied" errors**: Ensure proper HDFS permissions

   ```bash
   hdfs dfs -chmod 755 /user/student/us-accidents/
   ```

2. **Memory errors during processing**: Increase heap size in Hadoop configuration

   ```bash
   export HADOOP_HEAPSIZE=2048
   ```

3. **Job failures**: Check Hadoop logs in `$HADOOP_HOME/logs/`

### Verification Commands

```bash
# Check Hadoop services status
jps

# Verify HDFS directories
hdfs dfs -ls /user/student/us-accidents/

# Monitor job execution
yarn application -list
```

## Output Format

Results are saved to HDFS in `/user/student/us-accidents/outputs/` and displayed in the GUI with:

- Computed statistical values
- Processing time metrics
- Data distribution visualizations
- Summary statistics

## Future Enhancements

- Real-time streaming analytics integration
- Additional statistical functions (median, mode, percentiles)
- Machine learning model integration
- Enhanced visualization capabilities
- Multi-dataset comparative analysis

## Support

For technical issues or questions:

1. Check Hadoop cluster status and logs
2. Verify dataset format and upload completion
3. Ensure all required directories exist in HDFS
4. Review system requirements and dependencies
