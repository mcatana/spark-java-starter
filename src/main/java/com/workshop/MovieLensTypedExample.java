package com.workshop;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MovieLensTypedExample {

    public static void main(String[] args) {
        SparkSession sparkSession =SparkSession.builder()
                .master("local")
                .appName("MovieLensExample")
                .getOrCreate();

        String csvPath = "./src/main/resources/movielens/movies.csv";
        Dataset<Row> rowsDS = sparkSession.read()
                .format("csv")
                .option("inferSchema", true)
                .option("header","true")
                .load(csvPath);


        //Transformation to typed dataset. We use an encoder that translates
        // between jvm representations of java objects and Spark's internal binary format
        Dataset<Movie> moviesDS = rowsDS.as(Encoders.bean(Movie.class));

        moviesDS.printSchema();
        moviesDS.show(10, false);

        System.out.println("Movies with Paris");
        moviesDS.filter((FilterFunction<Movie>) movie -> movie.getTitle().toLowerCase().contains("paris"))
                .show(10, false);

        System.out.println("But we can also filter using untyped API:");
        moviesDS.filter("lower(title) like '%london%'").show(10, false);
    }

}
