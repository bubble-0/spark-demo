package spark.demo.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;

public class SparkRddDemo3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("demo");

        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("ERROR");
        JavaRDD<Integer> datas = jsc.parallelize(Arrays.asList(1, 2, 3, 1, 2, 3, 4, 5, 1));

        /**
         * 1.mapToPair
         * 2.flatMapToPair
         */
        datas.mapToPair(e -> new Tuple2<>(e, 1)).reduceByKey((e1, e2) -> e1 + e2).collect().forEach(e -> System.out.println(e));

        JavaRDD<String> fileDatas = jsc.textFile("datas/wc.txt");
        fileDatas.flatMapToPair(e -> {
            ArrayList list = new ArrayList();
            Arrays.asList(e.split(" ")).forEach(e2 -> {
                list.add(new Tuple2<>(e2, 1));
            });

            return list.iterator();
        });


        jsc.stop();

    }
}
