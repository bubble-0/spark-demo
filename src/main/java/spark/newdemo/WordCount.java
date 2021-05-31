package spark.newdemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]").setAppName("word count");

        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lineRdd = jsc.textFile("datas/wc.txt");

        JavaPairRDD<String, Integer> newRdd = lineRdd.flatMap(e -> {
            String[] arr = e.split(" ");
            return Arrays.asList(arr).iterator();
        }).mapToPair(e -> new Tuple2<>(e, 1)).reduceByKey((e1, e2) -> e1 + e2);

        newRdd.saveAsTextFile("datas/newwc");
        newRdd.foreach(e -> System.out.println(e));
        jsc.stop();

    }
}
