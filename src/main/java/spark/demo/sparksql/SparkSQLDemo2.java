package spark.demo.sparksql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;

public class SparkSQLDemo2 {
    public static class Person implements Serializable {
        String name;
        long age;

        public Person(String name, long age) {
            this.name = name;
            this.age = age;
        }

        public Person() {

        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getAge() {
            return age;
        }

        public void setAge(long age) {
            this.age = age;
        }
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("sparksql demo2").master("local[*]").getOrCreate();


        Person person = new Person();

        person.setName("zhangsan");
        person.setAge(18);

        // 1.固定类型dataset
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = spark.createDataset(
                Collections.singletonList(person),
                personEncoder
        );
        javaBeanDS.show();

//        Encoder<Person> personEncoder = Encoders.bean(Person.class);
//        Dataset<Person> dataset = spark.createDataset(Collections.singletonList(person), personEncoder);
//        dataset.show();

        // 2.基本类型dataset
        Encoder<Integer> anInt = Encoders.INT();
        Dataset<Integer> dataset1 = spark.createDataset(Arrays.asList(1, 2, 3, 4, 5, 6), anInt);
        dataset1.show();

        // 3.从json数据中获取并转化为dataset person类型
        Dataset<Person> as = spark.read().json("datas/person.json").as(personEncoder);
        as.show();
    }
}


