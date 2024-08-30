package org.kreps.iotdbhadoop;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;

public class HDFSToIoTDB {

    public static void main(String[] args) throws IOException {
        // Initialize Spark session
        SparkSession spark = SparkSession.builder()
                .appName("HDFS to IoTDB")
                .master("yarn")
                .config("spark.submit.deployMode", "cluster")
                .config("spark.sql.shuffle.partitions", "15")
                .getOrCreate();

        // HDFS base folder for data
        String hdfsBasePath = "hdfs://vm-1:9000/iotdb/";

        // FileSystem object to interact with HDFS
        FileSystem hdfs = FileSystem.get(spark.sparkContext().hadoopConfiguration());

        try {
            // List measurement directories inside /iotdb
            FileStatus[] measurementDirs = hdfs.listStatus(new Path(hdfsBasePath));

            // Iterate through each measurement directory
            for (FileStatus measurementDir : measurementDirs) {
                if (measurementDir.isDirectory()) {
                    String measurementName = measurementDir.getPath().getName();

                    // List subfolders inside each measurement directory
                    FileStatus[] dateRangeDirs = hdfs.listStatus(measurementDir.getPath());

                    // Iterate through each date range folder
                    for (FileStatus dateRangeDir : dateRangeDirs) {
                        if (dateRangeDir.isDirectory()) {
                            String dateRangeFolder = dateRangeDir.getPath().getName();
                            String parquetPath = dateRangeDir.getPath().toString() + "/*.parquet";

                            // Additional logging
                            System.out.println("Processing measurement: " + measurementName);
                            System.out.println("Processing date range folder: " + dateRangeFolder);

                            // Read the Parquet files from the date range folder
                            Dataset<Row> df = spark.read()
                                    .parquet(parquetPath);

                            // Additional logging
                            System.out.println("Loaded Parquet data for date range: " + dateRangeFolder);

                            String[] columns = df.columns();
                            for (String col : columns) {
                                if (col.startsWith("root.kreps.djn01")) {
                                    String newColName = col.replace("root.kreps.djn01", "root.kreps.test");
                                    df = df.withColumnRenamed(col, newColName);
                                }
                            }

                            // Write the data to IoTDB using the spark-iotdb-connector
                            df.write()
                                    .format("org.apache.iotdb.spark.db")
                                    .option("url", "jdbc:iotdb://192.168.0.202:6667/") // Adjust to your IoTDB instance
                                    .option("user", "root")
                                    .option("password", "root")
                                    .option("root_path", "root.kreps.test")
                                    .option("time_column", "Time")
                                    .mode("append")
                                    .save();

                            System.out.println("Successfully written data to IoTDB for date range: " + dateRangeFolder);
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error processing data: " + e.getMessage());
        } finally {
            spark.stop();
        }
    }
}
