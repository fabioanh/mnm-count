package com.test;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;

public class IfiTest {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("IfiTest")
                .master("local")
                .getOrCreate();

        String filterQuery = "SELECT publication_id FROM xml.t_patent_document_values";
        Dataset<Row> patentsDF = sparkSession.read()
                .format("jdbc")
                .option("user", "iplytics")
                .option("password", "a_password")
                .option("url", "jdbc:postgresql://localhost:5432/a_db")
                .option("dbtable", "xml.t_patent_document_values")
                .load();

        Dataset<Row> citedDocumentsDF = sparkSession.read()
                .format("jdbc")
                .option("user", "iplytics")
                .option("password", "a_password")
                .option("url", "jdbc:postgresql://localhost:5432/a_db")
                .option("dbtable", "cdws.t_cited_documents")
                .load();
//
//        Dataset<Row> filteredIdsDF = patentsDF.select(col("publication_id")) //.as("patent_publication_id")
//                .where(col("published").geq("1980-01-01").and(col("published").leq("1980-01-31")))
//                .limit(2)
//                .withColumnRenamed("publication_id", "patent_publication_id");

//        Dataset<Row> filteredIdsDF = sparkSession.read()
//                .format("jdbc")
//                .option("user", "iplytics")
//                .option("password", "a_password")
//                .option("url", "jdbc:postgresql://localhost:5432/a_db")
//                .option("dbtable", "xml.t_patent_document_values")
//                .load();
//                .where(col("published").geq("1980-01-01").and(col("published").leq("1980-01-31")))
//                .select(col("publication_id")) //.as("patent_publication_id")
//                .limit(2)
//                .withColumnRenamed("publication_id", "patent_publication_id");


        Dataset<Row> filteredIdsDF = patentsDF.select(col("publication_id")) //.as("patent_publication_id")
                .where(col("published").geq("1980-01-01").and(col("published").leq("1980-01-31")))
                .limit(2)
                .withColumnRenamed("publication_id", "patent_publication_id");

        filteredIdsDF.explain("simple");

        Dataset<Row> forwardCitationIds = filteredIdsDF
                .join(citedDocumentsDF, filteredIdsDF.col("patent_publication_id").equalTo(citedDocumentsDF.col("cited_document_id")))
//                .where(citedDocumentsDF.col("cited_document_id").equalTo(filteredIdsDF.col("patent_publication_id")))
                .select(citedDocumentsDF.col("publication_id"))
                .limit(2)
                ;

        forwardCitationIds.explain("simple");

        List<Integer> pubIds = filteredIdsDF.collectAsList().stream().map(Column::getItem).collect(Collectors.toList());
        Dataset<Row> forwardCitationIds2 = citedDocumentsDF
//                .join(filteredIdsDF, filteredIdsDF.col("patent_publication_id").equalTo(citedDocumentsDF.col("cited_document_id")))
//                .where(citedDocumentsDF.col("cited_document_id").equalTo(filteredIdsDF.col("patent_publication_id")))
                .filter(citedDocumentsDF.col("cited_document_id").isInCollection(pubIds))
                .select(citedDocumentsDF.col("publication_id"))
                .limit(2)
                ;


        forwardCitationIds2.explain("simple");
//        forwardCitationIds2.show(false);


        sparkSession.stop();
    }
}
