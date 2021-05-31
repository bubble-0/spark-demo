package spark.demo.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkDemo4 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]").setAppName("test");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> stringJavaRDD = jsc.textFile("datas/wc.txt");
        stringJavaRDD.saveAsTextFile("output");
        jsc.stop();
    }
}
