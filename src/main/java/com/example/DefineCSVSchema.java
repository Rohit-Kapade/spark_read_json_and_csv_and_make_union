package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DefineCSVSchema {

    public void  printDefinedSchema() {
        SparkSession ss = SparkSession.builder()
                .master("local")
                .appName("Define CSV Schema")
                .getOrCreate();

        StructType customSchema = DataTypes.createStructType(
                new StructField[] {
            DataTypes.createStructField("id",DataTypes.IntegerType,false),
                    DataTypes.createStructField("product_id",DataTypes.IntegerType,true),
                    DataTypes.createStructField("item_name",DataTypes.StringType,false),
                    DataTypes.createStructField("published_on",DataTypes.DateType,true),
                    DataTypes.createStructField("url",DataTypes.StringType,false)
        });

        Dataset<Row> df = ss.read().format("csv")
                .option("header", "true")
                .option("multiline", true)
                .option("sep", ";")
                .option("quote", "^")
                .option("dateFormat", "M/d/y")
                //.option("inferSchema", false) // false does not work
                .option("_corrupt_record", true) // The schema contains a special column _corrupt_record, which does not exist in the data. This column captures rows that did not parse correctly.
                .schema(customSchema) // no schema inferrence, use schema() API
                .load("src/main/resources/amazonProducts.txt");

        System.out.println("Dataframe content : ");
        df.show(7);
        //df.show(7,90);
        System.out.println("Dateframe's Defined Schema : ");
        df.printSchema();

    }
}
