package com.workshop;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Array;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lower;

public class MovieLensExample {

    public static void main(String[] args) {
        SparkSession sparkSession =SparkSession.builder()
                .master("local")
                .appName("MovieLensTypedExample")
                .getOrCreate();


        String csvPath = "./src/main/resources/movielens/movies.csv";
        Dataset<Row> moviesDS = sparkSession.read()
                .format("csv")
                .option("header","true")
                .option("inferSchema", true)
                .load(csvPath);

        moviesDS.printSchema();

        moviesDS.show(10, false);

        System.out.println("Filter using untyped API");
        moviesDS.filter("lower(title) like '%paris%'").show(10, false);

        System.out.println("Another way to filter data using untyped API");
        moviesDS.filter(lower(col("genres")).contains("comedy")).show(10, false);
    }

}
