package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class InferCSVSchema {

    public void  printSchema(){
        SparkSession ss = SparkSession.builder()
                .appName("Complex CSV to DataFrames")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = ss.read().format("csv")
                .option("header", "true")
                .option("multiline", true)
                .option("sep", ";")
                .option("quote", "^")
                .option("dateFormat", "M/d/y")
                .option("inferSchema", true)
                .load("src/main/resources/amazonProducts.txt");

        System.out.println("Excert of the dataframe content : ");
        df.show(7);
        //df.show(7,90);
        System.out.println("Dateframe's Schema : ");
        df.printSchema();
    }

}
