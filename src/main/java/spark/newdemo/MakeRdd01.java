package spark.newdemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class MakeRdd01 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName(" make rdd ").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        /**
         * 创建rdd
         */

        //1. 从集合创建
        Integer[] arr = {1, 2, 3, 4, 5, 6};
        List<Integer> list = Arrays.asList(arr);
        JavaRDD<Integer> arrRdd = jsc.parallelize(list);
        arrRdd.foreach(e -> System.out.println(e));

        //2.从外部文件创建
        JavaRDD<String> dataRdd = jsc.textFile("datas/data.txt");
        dataRdd.foreach(e -> System.out.println(e));

        //3.从其他RDD创建 : 从其他rdd经过计算返回一个新的rdd

        //4.直接创建, spark框架自身使用

        jsc.stop();
    }
}
