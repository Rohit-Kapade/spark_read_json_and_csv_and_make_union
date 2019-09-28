package com.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;


public class SparkAppMain {

    final static String jdbcUrl = "jdbc:mysql://localhost:3306/world";
    final static String mysqlUsername = "root";
    final static String mysqlPassword = "root";

    public static void main(String[] args) throws IOException {
//        SparkConf sparkConf = new SparkConf()
//                .setAppName("Example Spark App")
//                .setMaster("local[*]");  // Delete this line when submitting to a cluster
//        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
//        JavaRDD<String> stringJavaRDD = sparkContext.textFile("D:\\project\\Spark_data\\aadhaar_data.csv");
//        System.out.println("Number of lines in file = " + stringJavaRDD.count());

        //create a SparkSession
        SparkSession ss = new SparkSession.Builder()
                .appName("CSV to DB")
                .master("local")
                .getOrCreate();

        //ss.sparkContext().setLogLevel("WARN");  // This does not work
        Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);

        Dataset<Row> durhamDf = buildDurhamParkData(ss);
//        durhamDf.printSchema();
//        durhamDf.show(10);

        Dataset<Row> philadelphiaDf = buildPhilRecreationData(ss);
//        philadelphiaDf.printSchema();
//        philadelphiaDf.show(10);


        Dataset<Row> unionDf = unionTwoDataset(philadelphiaDf, durhamDf);
//        unionDf.printSchema();
//        unionDf.show(10);

        Partition [] partitions = unionDf.rdd().getPartitions();
        System.out.println("Number of partitions : " + partitions.length + " in Union DF !");

        unionDf = unionDf.repartition(5);
        System.out.println("New no. of partitions : " + unionDf.rdd().getPartitions().length + " in Union DF !");


//      Dataset<Row> df = ss.read().format("csv")
//                .option("header", true)
//                .load("src/main/resources/name_and_comments.txt");
//
//        df = df.withColumn("full_name", functions.concat(df.col("last_name"), functions.lit(", "), df.col("first_name")));
//
//        Dataset<Row> transformedDF = df.filter(df.col("comment").rlike("[0-9]"))
//                .orderBy(df.col("last_name")
//                        .asc());
//        transformedDF.show();
//
//        Connection conn = null;
//        try {
//            conn=DriverManager.getConnection(jdbcUrl,mysqlUsername,mysqlPassword);
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//
//        Properties connectionProperties = new Properties();
//        connectionProperties.put("user", mysqlUsername);
//        connectionProperties.put("password", mysqlPassword);
//
////        df.write()
////                .mode(SaveMode.Overwrite)
////                .jdbc(jdbcUrl, "name_and_comments", connectionProperties);
//
//
//        Dataset<Row> world_city_table = ss.read().jdbc(jdbcUrl, "name_and_comments", connectionProperties);
//
//        world_city_table.show();

//        InferCSVSchema csvParser = new InferCSVSchema();
//        csvParser.printSchema();
//
//        DefineCSVSchema csvParser = new DefineCSVSchema();
//        csvParser.printDefinedSchema();

//        JSONLinersParser jsonParser = new JSONLinersParser();
//        jsonParser.printJSONschema();
    }

    private static Dataset<Row> unionTwoDataset(Dataset<Row> df1, Dataset<Row> df2) {
        Dataset<Row> df= df1.unionByName(df2);
        df.printSchema();
        df.show(500);
        System.out.println("Number of union " + df.count() + "records !!");
        return df;
    }

    private static Dataset<Row> buildPhilRecreationData(SparkSession ss) {
        Dataset<Row> df = ss.read().format("csv")
                .option("header", "true")
                .option("multiline", true)
                .option("inferSchema", true)
                .load("src/main/resources/philadelphia_recreations.csv");

        df = df.filter(functions.lower(df.col("USE_")).like("%park%"));
        //df = df.filter("lower(USE_) like '%park%' ");

        df = df.withColumn("park_id", functions.concat(functions.lit("phil_"), df.col("OBJECTID")))
                .withColumnRenamed("ASSET_NAME", "park_name")
                .withColumn("city", functions.lit("Philadelphia"))
                .withColumnRenamed("ADDRESS", "address")
                .withColumn("has_playground", functions.lit("UNKNOWN"))
                .withColumnRenamed("ZIPCODE", "zipcode")
                .withColumnRenamed("ACREAGE", "land_in_acres")
                .withColumn("geoX", functions.lit("UNKNONW"))
                .withColumn("geoY", functions.lit("UNKNONW"))
                .drop("SITE_NAME")
                .drop("OBJECTID")
                .drop("CHILD_OF")
                .drop("TYPE")
                .drop("USE_")
                .drop("DESCRIPTION")
                .drop("SQ_FEET")
                .drop("ALLIAS")
                .drop("CHRONOLOGY")
                .drop("NOTES")
                .drop("DATE_EDITED")
                .drop("EDITED_BY")
                .drop("OCCUPANT")
                .drop("TENANT")
                .drop("LABEL");

        return df;
    }

    private static Dataset<Row> buildDurhamParkData(SparkSession ss) {
        Dataset<Row> df = ss.read().format("json")
                .option("multiline",true)
                .load("src/main/resources/durham-parks.json");

        df = df.withColumn("park_id",
                functions.concat(df.col("datasetid"), functions.lit("_"), df.col("fields.objectid"), functions.lit("_Durham")))
                .withColumn("park_name", df.col("fields.park_name"))
                .withColumn("city", functions.lit("Durham"))
                .withColumn("address", df.col("fields.address"))
                .withColumn("has_playground", df.col("fields.playground"))
                .withColumn("zipcode", df.col("fields.zip"))
                .withColumn("land_in_acres", df.col("fields.acres"))
                .withColumn("geoX", df.col("geometry.coordinates").getItem(0))
                .withColumn("geoY", df.col("geometry.coordinates").getItem(1))
                .drop("datasetid").drop("fields").drop("geometry").drop("record_timestamp").drop("recordid");

        return df;
    }
}
