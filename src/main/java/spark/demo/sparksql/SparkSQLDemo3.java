package spark.demo.sparksql;

import com.sun.org.apache.bcel.internal.generic.LSTORE;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;

import java.sql.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class SparkSQLDemo3 {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("sparksql demo3").master("local[2]").getOrCreate();

        JavaRDD<String> stringJavaRDD = spark.read().textFile("datas/wc.txt").javaRDD();
        JavaRDD<String> stringJavaRDD1 = stringJavaRDD.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<String> call(Iterator<String> stringIterator) throws Exception {
                List list = new ArrayList<String>();
                while (stringIterator.hasNext()) {
                    String[] arr = stringIterator.next().split(" ");
                    for (String str : arr) {
                        list.add(str);
                    }
                }
                return list.iterator();
            }
        });

        stringJavaRDD1.foreach(e -> System.out.println(e));

//        List<String> names = Arrays.asList("w1", "w2", "w3", "w4");
//        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
//        JavaRDD<String> nameRdd = jsc.parallelize(names);
//        JavaRDD<Integer> result = nameRdd.mapPartitions(new FlatMapFunction<Iterator<String>, Integer>() {
//            private static final long serialVersionUID = 1L;
//
//            @Override
//            public Iterator<Integer> call(Iterator<String> iterator) throws Exception {
//                List<Integer> list = new ArrayList<>();
//                while (iterator.hasNext()) {
//                    String name = iterator.next();
//                    System.out.println(name);
//                }
//                return list.iterator();
//            }
//        });
//        result.collect();
//        result.foreach(e -> System.out.println(e));
    }
}
