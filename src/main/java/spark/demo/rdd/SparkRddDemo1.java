package spark.demo.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class SparkRddDemo1 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("wc");

        JavaSparkContext sc = new JavaSparkContext(conf);
        /**
         * 创建rdd的方法 :
         *   1.parallelize  方法最后一个参数指定分区
         *   2.makeRDD scala才有 java没有
         *   3.textFile  方法最后一个参数指定分区
         */

        // 1.把一个集合转换成RDD
        JavaRDD<Integer> parallelize = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7));

        // 2.从文件中读取 textFile
        JavaRDD<String> stringJavaRDD = sc.textFile("datas/wc.txt");

        //parallelize.foreach(e-> System.out.println(e));

        //stringJavaRDD.foreach(e-> System.out.println(e));

        /**
         *  1.filter 过滤
         *  2.map 转换
         *  3.flatMap 扁平化处理
         */

        parallelize.filter(e -> (e % 2) == 0).foreach(e -> System.out.println(e));

        parallelize.map(e -> e * 2).foreach(e -> System.out.println(e));

        stringJavaRDD.flatMap(e->Arrays.asList(e.split(" ")).iterator()).foreach(e -> System.out.println(e));

        sc.stop();
    }
}
