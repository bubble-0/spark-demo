package spark.newdemo;

import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.CollectionsUtils;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class KeyValueRdd03 {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf();
        conf.setAppName(" KeyValue Rdd ").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1, 1, 6, 4, 5, 6), 3);

        JavaPairRDD<Integer, Integer> pairRdd = rdd.mapToPair(e -> new Tuple2<>(e, 1));
        //pairRdd.saveAsTextFile("datas/pairRdd");

        /**
         * key value 类型算子
         */
        // partitionBy:将数据按照指定 Partitioner 重新进行分区。Spark 默认的分区器是 HashPartitioner
        JavaPairRDD<Integer, Integer> newHashPairRdd = pairRdd.partitionBy(new HashPartitioner(6));
        //newHashPairRdd.saveAsTextFile("datas/newPairRdd");

        // partitionBy自定义分区器
        JavaPairRDD<Integer, Integer> myPartitionRdd = pairRdd.partitionBy(new MyPartitioner(rdd.map(e -> e.toString()).collect(), 6));
        //myPartitionRdd.saveAsTextFile("datas/myPairtition");
        System.out.println("新分区数" + myPartitionRdd.getNumPartitions());
        System.out.println("新分区集合" + myPartitionRdd.partitions());
        System.out.println("新分区器" + myPartitionRdd.partitioner().toString());

        // reduceByKey: 可以将数据按照相同的 Key 对 Value 进行聚合
        // 相同key进行计算, 参数e1  e2是相同key的每个value 的值
        JavaPairRDD<Integer, Integer> reduceRdd = pairRdd.reduceByKey((e1, e2) -> e1 + e2);
        reduceRdd.foreach(e -> System.out.println(" reduceByKey: " + e));

        // groupByKey: 将数据源的数据根据 key 对 value 进行分组
        // 第二个参数参数可以设置并行度,如果不设置默认和父rdd一致, 也可以从新分区
        JavaPairRDD<Integer, Iterable<Integer>> groupPairRdd = pairRdd.groupByKey();
        groupPairRdd.foreach(e -> System.out.println(" groupByKey: " + e));

        JavaPairRDD<Integer, Iterable<Integer>> groupPairRdd2 = pairRdd.groupByKey(2);
        groupPairRdd2.foreach(e -> System.out.println(" groupByKey2: " + e));

        // aggregateByKey: 将数据根据不同的规则进行分区内计算和分区间计算
        // 第一个参数是计算的初始值, 第二个是分区内计算, 第三个是分区间计算
        JavaPairRDD<Integer, Integer> aggregatePairRdd = pairRdd.aggregateByKey(0, (e1, e2) -> Math.max(e1, e2), (e1, e2) -> e1 + e2);
        pairRdd.saveAsTextFile("datas/pairRdd");
        aggregatePairRdd.foreach(e -> System.out.println("aggregatePairRdd " + e));
    }


    static class MyPartitioner extends Partitioner {
        private int partitions;

        private Map<Object, Integer> map = new ConcurrentHashMap<>();

        MyPartitioner(List<String> keys, int partitionsNum) throws Exception {
            if (CollectionUtils.isEmpty(keys)) {
                throw new Exception("keys 不能为空!");
            }
            this.partitions = partitionsNum < 0 ? keys.size() : partitionsNum;
            for (int i = 0; i < keys.size(); i++) {
                //这里提前把每个key算好分在哪个分区了 写0 就是0分区 1 就是1分区 -1 就会报错
                map.put(keys.get(i), i % this.partitions);
            }
        }

        @Override
        public int numPartitions() {
            return this.partitions;
        }

        @Override
        public int getPartition(Object key) {
            return map.get(key.toString());
        }

        @Override
        public boolean equals(Object o) {
//            if (this == o) return true;
//            if (o == null || getClass() != o.getClass()) return false;
//            MyPartitioner that = (MyPartitioner) o;
//            return partitions == that.partitions &&
//                    Objects.equals(map, that.map);
            if (o instanceof MyPartitioner) {
                return ((MyPartitioner) o).partitions == this.partitions;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return this.partitions;
        }
    }

}
