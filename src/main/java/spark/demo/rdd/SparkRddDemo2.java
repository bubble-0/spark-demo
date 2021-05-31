package spark.demo.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class SparkRddDemo2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("demo");

        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("ERROR");
        JavaRDD<Integer> distinct = jsc.parallelize(Arrays.asList(1, 1, 2, 2, 3));
        JavaRDD<Integer> union = jsc.parallelize(Arrays.asList(3,4, 5, 6));


        /**
         * distinct 去重
         * union 并集
         * intersection rdd1和rdd2的交集
         * subtract 在rdd1中出现但是不在rdd2中出现的数据
         * cartesian rdd1和rdd2的笛卡尔积
         */

        System.out.print("去重");
        distinct.distinct().collect().forEach(e-> System.out.println(e));

        System.out.print("并集");
        distinct.union(union).collect().forEach(e-> System.out.println(e));

        System.out.print("交集");
        distinct.intersection(union).collect().forEach(e-> System.out.println(e));

        System.out.print("差集");
        distinct.subtract(union).collect().forEach(e-> System.out.println(e));

        System.out.print("笛卡尔积");
        distinct.cartesian(union).collect().forEach(e-> System.out.println(e));


        jsc.stop();

    }
}
