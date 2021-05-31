package spark.demo.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class SparkAccDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("lcoal[*]").setAppName("acc");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<Integer> arr = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6));
        int num = 0;

//        arr.foreach(e -> num = num + e);


        jsc.stop();
    }
}
