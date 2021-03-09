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
public class FireCalls {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("FireCalls")
                .master("local")
                .getOrCreate();
        List<StructField> schemaFields = new ArrayList<>();
        schemaFields.add(DataTypes.createStructField("CallNumber", DataTypes.IntegerType, true));
        schemaFields.add(DataTypes.createStructField("UnitID", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("IncidentNumber", DataTypes.IntegerType, true));
        schemaFields.add(DataTypes.createStructField("CallType", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("CallDate", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("WatchDate", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("CallFinalDisposition", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("AvailableDtTm", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("Address", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("City", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("Zipcode", DataTypes.IntegerType, true));
        schemaFields.add(DataTypes.createStructField("Battalion", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("StationArea", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("Box", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("OriginalPriority", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("Priority", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("FinalPriority", DataTypes.IntegerType, true));
        schemaFields.add(DataTypes.createStructField("ALSUnit", DataTypes.BooleanType, true));
        schemaFields.add(DataTypes.createStructField("CallTypeGroup", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("NumAlarms", DataTypes.IntegerType, true));
        schemaFields.add(DataTypes.createStructField("UnitType", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("UnitSequenceInCallDispatch", DataTypes.IntegerType, true));
        schemaFields.add(DataTypes.createStructField("FirePreventionDistrict", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("SupervisorDistrict", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("Neighborhood", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("Location", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("RowID", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("Delay", DataTypes.FloatType, true));
        StructType schema = DataTypes.createStructType(schemaFields.toArray(new StructField[0]));

        Dataset<Row> fireCallsDF = sparkSession.read()
                .schema(schema)
                .option("header", true)
                .csv(args[0]);

        Dataset<Row> sampleFilter = fireCallsDF.select("IncidentNumber", "AvailableDtTm", "CallType")
                .where(fireCallsDF.col("CallType").notEqual("Medical Incident"));

        sampleFilter.show(5, false);

        fireCallsDF.select("CallType")
                .where(col("CallType").isNotNull())
                .agg(countDistinct(col("CallType")).alias("DistinctCallTypes"))
                .show(1, false);

        fireCallsDF.select("CallType")
                .where(col("CallType").isNotNull())
                .distinct()
                .show(10, false);

        fireCallsDF.select("CallType")
                .where(col("CallType").isNotNull())
                .distinct()
                .show(10, false);

        Dataset<Row> fireCallsTSDF = fireCallsDF.withColumn("IncidentDate", to_timestamp(fireCallsDF.col("CallDate"), "MM/dd/yyyy"))
                .drop("CallDate")
                .withColumn("OnWatchDate", to_timestamp(fireCallsDF.col("WatchDate"), "MM/dd/yyyy"))
                .drop("WatchDate")
                .withColumn("AvailableDtTS", to_timestamp(fireCallsDF.col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))
                .drop("AvailableDtTm");

        fireCallsTSDF.select("IncidentDate", "OnWatchDate", "AvailableDtTS").show(5, false);

        fireCallsTSDF.select(year(fireCallsTSDF.col("IncidentDate"))).distinct().orderBy(year(fireCallsTSDF.col("IncidentDate")).desc()).show();

        sparkSession.stop();
    }
}
