package com.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("JavaMnMCount")
                .master("local")
                .getOrCreate();
//        Dataset<Row> mnmDS = sparkSession.read().csv(args[0]);
        Dataset<Row> mnmDS = sparkSession.read()
                .format("csv")
                .option("header", true)
                .option("inferSchema", true)
                .load(args[0]);

//        mnmDS.select(col)
        Dataset<Row> countMnMDS = mnmDS.select(col("State"), col("Color"), col("Count"))
                .groupBy(col("State"), col("Color"))
                .agg(count(col("Count")).alias("Total"))
                .orderBy(desc("Total"));

        countMnMDS.show(60, false);

        System.out.println(String.format("Total rows = %d", countMnMDS.count()));

        Dataset<Row> caCountMnMDS = mnmDS.select(col("State"), col("Color"), col("Count"))
//                .filter("State == CA")
                .where(mnmDS.col("State").equalTo("CA"))
                .groupBy(col("State"), col("Color"))
                .agg(count(col("Count")).alias("Total"))
                .orderBy(desc("Total"));
        caCountMnMDS.show(10, false);

        sparkSession.stop();
    }
}
