package org.kreps.iotdbhadoop;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Date;
import java.util.TimeZone;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class IoTDBToHDFS {

    private static final int CHUNK_SIZE = 60 * 60 * 24;

    public static void main(String[] args) {
        // Initialize Spark session
        SparkSession spark = SparkSession.builder()
                .appName("IoTDB to HDFS")
                .master("yarn")
                .config("spark.submit.deployMode", "cluster")
                .config("spark.sql.shuffle.partitions", "15")
                .getOrCreate();

        // Example input parameters
        String startDateStr = "2023-07-06T00:00:00.000";
        String endDateStr = "2024-07-05T00:00:00.000";
        String[] measurements = { "temperature", "humidity", "pressure", "light", "frequency" };

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

        try {
            // Convert date strings to Date objects
            Date startDate = sdf.parse(startDateStr);
            Date endDate = sdf.parse(endDateStr);

            // Iterate through each measurement
            for (String measurement : measurements) {
                // Calculate the total number of points
                long totalSeconds = (endDate.getTime() - startDate.getTime()) / 1000;
                int numChunks = (int) Math.ceil(totalSeconds / CHUNK_SIZE);

                System.out.println("Processing measurement: " + measurement);
                System.out.println("Total chunks: " + numChunks);

                for (int i = 0; i < numChunks; i++) {
                    // Calculate chunk start and end times
                    long chunkStartMillis = startDate.getTime() + (i * CHUNK_SIZE * 1000L);

                    // Ensure we don't exceed the end date
                    if (chunkStartMillis >= endDate.getTime()) {
                        break; // Stop if we're beyond the end date
                    }

                    long chunkEndMillis = Math.min(chunkStartMillis + (CHUNK_SIZE * 1000), endDate.getTime());
                    Date chunkStartDate = new Date(chunkStartMillis);
                    Date chunkEndDate = new Date(chunkEndMillis);

                    // Format dates for SQL query
                    String chunkStartDateStr = sdf.format(chunkStartDate);
                    String chunkEndDateStr = sdf.format(chunkEndDate);

                    // Additional logging
                    System.out.println("Chunk #" + i);
                    System.out.println("Start date: " + chunkStartDateStr);
                    System.out.println("End date: " + chunkEndDateStr);
                    System.out.println("Chunk start millis: " + chunkStartMillis);
                    System.out.println("Chunk end millis: " + chunkEndMillis);

                    // Build and execute SQL query
                    String sqlQuery = String.format(
                            "SELECT %s FROM root.kreps.djn01 WHERE time >= %s AND time < %s",
                            measurement, chunkStartDateStr, chunkEndDateStr);

                    // Additional logging
                    System.out.println("SQL Query: " + sqlQuery);

                    // Call method to read and write data
                    readAndWriteData(spark, sqlQuery, measurement, chunkStartDateStr, chunkEndDateStr);
                }
            }
        } catch (ParseException e) {
            System.err.println("Date parsing error: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        } finally {
            spark.stop();
        }
    }

    private static void readAndWriteData(SparkSession spark, String sqlQuery, String measurement,
            String chunkStartDateStr, String chunkEndDateStr) {
        try {
            Dataset<Row> df = spark.read()
                    .format("org.apache.iotdb.spark.db")
                    .option("url", "jdbc:iotdb://192.168.0.202:6667/")
                    .option("user", "root")
                    .option("password", "root")
                    .option("sql", sqlQuery)
                    .load();

            String formattedStartDate = formatTimestampForPath(chunkStartDateStr);
            String formattedEndDate = formatTimestampForPath(chunkEndDateStr);

            String path = "/iotdb/" + measurement + "/" + formattedStartDate + "-" + formattedEndDate;
            String hdfsPath = "hdfs://vm-1:9000" + path;

            // Write DataFrame to HDFS in parquet format
            df.write()
                    .mode("overwrite") // You might want to use "append" in a production environment
                    .parquet(String.format(hdfsPath));
        } catch (Exception e) {
            System.err.println("Error reading/writing data: " + e.getMessage());
        }
    }

    public static String formatTimestampForPath(String timestamp) {
        return timestamp.replace(":", "-").replace("T", "_").replace(".", "_");
    }
}
