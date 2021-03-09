package com.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * Hello world!
 */
public class SchemaTest {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("SchemaTest")
                .master("local")
                .getOrCreate();
        List<StructField> schemaFields = new ArrayList<>();
        schemaFields.add(DataTypes.createStructField("Id", DataTypes.IntegerType, true));
        schemaFields.add(DataTypes.createStructField("First", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("Last", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("Url", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("Published", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("Hits", DataTypes.IntegerType, true));
        schemaFields.add(DataTypes.createStructField("Campaigns", DataTypes.createArrayType(DataTypes.StringType), true));
        StructType schema = DataTypes.createStructType(schemaFields.toArray(new StructField[0]));

        Dataset<Row> jsonDS = sparkSession.read().schema(schema).json(args[0]);

        jsonDS.show(false);
        jsonDS.printSchema();
        System.out.println(jsonDS.schema());

        System.out.println("\n---");
        System.out.println(jsonDS.col("Hits"));
        jsonDS.select(jsonDS.col("Hits").multiply(2)).show(2);
        jsonDS.select(expr("Hits * 2")).show(2);

        jsonDS.withColumn("Big Hitters", (expr("Hits > 10000"))).show();

        jsonDS.withColumn("Authors Id", concat_ws(" ", expr("First"), expr("Last"), expr("Id")))
                .select("Authors Id")
                .show(4);
        jsonDS.sort(desc("Id")).show();
        jsonDS.sort(col("Id").desc()).show();

        sparkSession.stop();
    }
}
