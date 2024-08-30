# IoTDB-Hadoop Connector

This repository contains two Java programs that facilitate the integration between IoTDB and Hadoop. These programs allow for data migration between IoTDB and HDFS, supporting both directions: from IoTDB to HDFS and from HDFS back to IoTDB.

## Overview

The repository includes two main Java files:

1. **IoTDBToHDFS.java**: Reads data from IoTDB and writes it to HDFS in Parquet format.
2. **HDFSToIoTDB.java**: Reads data from HDFS (in Parquet format) and writes it back to IoTDB.

## Prerequisites

- **Apache IoTDB**: The time-series database where data is stored and retrieved.
- **Apache Hadoop HDFS**: The distributed file system where data is stored in Parquet format.
- **Apache Spark**: Used to process and transfer data between IoTDB and HDFS.
- **Java 8**: Required for running the Java programs.

## Usage

### IoTDB to HDFS

The `IoTDBToHDFS` program reads time-series data from IoTDB and writes it to HDFS. The data is processed in chunks, and each chunk corresponds to a specific time range. 

#### Running the Program

1. **Configure the Input Parameters**: 
   - Modify the start date, end date, and measurements in the `IoTDBToHDFS.java` file as per your requirements.

2. **Run the Program**:
   - Use the following command to run the program:
     ```bash
     spark-submit --class org.kreps.iotdbhadoop.IoTDBToHDFS path/to/your/jarfile.jar
     ```
   - The program will generate Parquet files in the specified HDFS directory.

### HDFS to IoTDB

The `HDFSToIoTDB` program reads data from HDFS (in Parquet format) and writes it back to IoTDB. It iterates over directories in HDFS, processes the Parquet files, and writes the data to the specified IoTDB database.

#### Running the Program

1. **Configure the HDFS Path**:
   - Set the HDFS base path in the `HDFSToIoTDB.java` file.

2. **Run the Program**:
   - Use the following command to run the program:
     ```bash
     spark-submit --class org.kreps.iotdbhadoop.HDFSToIoTDB path/to/your/jarfile.jar
     ```
   - The program will read Parquet files from HDFS and write the data to IoTDB.

## Notes

- **Error Handling**: Both programs include basic error handling. If any issues occur during execution, appropriate error messages will be logged to the console.
- **Configuration**: The programs are configured to run on a YARN cluster in cluster mode. Adjust the Spark configuration in the `SparkSession.builder()` as needed.
- **Logging**: Additional logging has been added to track the progress of data processing, including the start and end times for each chunk of data.
