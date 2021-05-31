package spark.newdemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class ValueRdd02 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]").setAppName("  value 算子  ");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        Integer[] arr = {1, 2, 3, 4, 5, 6, 7, 8, 9};
        List<Integer> list = Arrays.asList(arr);

        JavaRDD<Integer> rdd = jsc.parallelize(list, 3);

        /**
         * value型转换算子
         */

        //1. map: 将处理的数据逐条进行映射转换，这里的转换可以是类型的转换，也可以是值的转换。
        JavaRDD<Integer> mapRdd = rdd.map(e -> e * 2);
        mapRdd.foreach(e -> System.out.println("map" + e));

        //2. mapPartitions :将待处理的数据以分区为单位发送到计算节点进行处理，这里的处理是指可以进行任意的处
        //理，哪怕是过滤数据。
        JavaRDD<Integer> mapPartitionsRdd = rdd.mapPartitions(e -> {
            List<Integer> newList = new ArrayList<>();
            while (e.hasNext()) {
                newList.add(e.next() * 3);
            }
            return newList.iterator();
        });
        mapPartitionsRdd.foreach(e -> System.out.println("mapPartitions" + e));

        //3. mapPartitionsWithIndex: 将待处理的数据以分区为单位发送到计算节点进行处理，这里的处理是指可以进行任意的处
        //理，哪怕是过滤数据，在处理时同时可以获取当前分区索引。
        JavaRDD<Tuple2<Integer, Iterator<Integer>>> tuple2JavaRDD = rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<Tuple2<Integer, Iterator<Integer>>>>() {
            @Override
            public Iterator<Tuple2<Integer, Iterator<Integer>>> call(Integer v1, Iterator<Integer> v2) throws Exception {
                List<Tuple2<Integer, Iterator<Integer>>> newList = new ArrayList<>();
                newList.add(new Tuple2<>(v1, v2));
                return newList.iterator();
            }
        }, true);
        tuple2JavaRDD.foreach(e -> System.out.println("mapPartitionsWithIndex" + e._1 + "  " + e._2.toString()));

        //4. flatMap: 将处理的数据进行扁平化后再进行映射处理，所以算子也称之为扁平映射
        // word count
        JavaRDD<String> textRdd = jsc.textFile("datas/wc.txt");
        textRdd.flatMap(line -> {
            String[] s = line.split(" ");
            return Arrays.asList(s).iterator();
        }).mapToPair(e -> new Tuple2<>(e, 1)).reduceByKey((e1, e2) -> e1 + e2);

        //5. glom:将同一个分区的数据直接转换为相同类型的内存数组进行处理，分区不变
        JavaRDD<List<Integer>> glomRdd = rdd.glom();
        glomRdd.foreach(e -> System.out.println(e));

        //6. groupBy:将数据根据指定的规则进行分组, 分区默认不变，但是数据会被打乱重新组合，我们将这样
        //的操作称之为 shuffle。极限情况下，数据可能被分在同一个分区中
        //一个组的数据在一个分区中，但是并不是说一个分区中只有一个组
        JavaPairRDD<Integer, Iterable<Integer>> groupByRdd = rdd.groupBy(e -> e % 3);
        groupByRdd.foreach(e -> System.out.println(e._1 + "   " + e._2));

        jsc.stop();
    }
}
