package spark.demo.sparksql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class SparkSQLDemo1 {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Spark Sql basic example").master("local").getOrCreate();
        //这个dataset 没有类型自动识别
        Dataset<Row> json = spark.read().json("datas/user.json");
        json.show();

        json.printSchema();

        json.select("username").show();

        json.select(col("username"), col("age").plus(1)).show();

        json.select(col("username"), col("age").gt(18)).show();

        json.filter(col("age").gt(18)).show();

        json.groupBy(col("age")).count().show();

        json.createOrReplaceTempView("people");

        Dataset<Row> sql = spark.sql("select * from people");
        sql.show();

    }
}
