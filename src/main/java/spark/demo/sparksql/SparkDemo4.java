package spark.demo.sparksql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDemo4 {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("spark demo4").master("local").getOrCreate();
        JavaRDD<Person> rdd = spark.read().textFile("datas/person.txt").javaRDD().map(e -> {
            Person person = new Person();
            String[] arr = e.split(" ");
            person.setName(arr[0]);
            person.setAge(Integer.parseInt(arr[1]));
            return person;
        });

        Dataset<Row> dataFrame = spark.createDataFrame(rdd, Person.class);
        dataFrame.createOrReplaceTempView("person");
        Dataset<Row> sql = spark.sql(" select * from person ");
        sql.printSchema();
        sql.show();




        spark.stop();
    }
}

class Person {
    String name;
    int age;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
