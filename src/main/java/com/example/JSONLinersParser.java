package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JSONLinersParser {
    public void printJSONschema(){

        SparkSession ss = SparkSession.builder()
                .appName("JSON to Dataframe builder")
                .master("local")
                .getOrCreate();

        Dataset<Row> dfSimple = ss.read().format("json")
                .load("src/main/resources/simple.json");

        Dataset<Row> dfMultiline = ss.read().format("json")
                .option("multiline", true)
                .option("_corrupt_record",true)
                .load("src/main/resources/multiline.json");

        //dfSimple.show(5);
        dfSimple.show(5,150);
        dfSimple.printSchema();

        dfMultiline.show(5,150);
        dfMultiline.printSchema();
    }
}
