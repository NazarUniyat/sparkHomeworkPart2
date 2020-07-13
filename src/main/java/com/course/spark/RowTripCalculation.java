package com.course.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;

public class RowTripCalculation {
    public static void main(String[] args) {

        String path = "You have to input path!";
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkSession session = SparkSession.builder()
                .appName("yellow-trip-data-calculation")
                .master("local[*]")
                .getOrCreate();
        DataFrameReader dataFrameReader = session.read();

        Dataset<Row> selected = dataFrameReader.option("header", "true")
                .csv(path)
                .select(
                        col("tpep_pickup_datetime"),
                        col("passenger_count").cast("integer"),
                        col("trip_distance").cast("float"),
                        col("total_amount").cast("float")
                );


        Dataset<Row> withMappedDate = selected.map((MapFunction<Row, Row>) row ->
                RowFactory.create(
                        FieldMapper.mapDateTime(row.<String>getAs("tpep_pickup_datetime")),
                        row.<Integer>getAs("passenger_count"),
                        row.<Float>getAs("trip_distance"),
                        row.<Float>getAs("total_amount")
                ), RowEncoder.apply(selected.schema()));

        Dataset<Row> filtered = withMappedDate
                .filter(col("passenger_count").equalTo(1));

        Dataset<Row> rowAggregated = filtered
                .groupBy(col("tpep_pickup_datetime"))
                .agg(avg("trip_distance"), avg("total_amount"));
        rowAggregated.show();
    }

}
