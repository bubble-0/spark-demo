package spark.demo.rdd;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Demo {
    public static void main(String[] args) {
        String[] arr = {"ff", "ee", "dd", "aa", "cc", "cc", "bb", "aa"};
        Map<String, List<String>> collect = Stream.of(arr).collect(Collectors.groupingBy(e -> e));
        collect.entrySet().stream().forEach(e-> {
            System.out.println(e.getKey() + "   " + e.getValue().size());
        });
        System.out.println(collect);
    }
}
