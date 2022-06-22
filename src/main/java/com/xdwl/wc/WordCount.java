package com.xdwl.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> LineData = env.readTextFile("inout.txt");

        FlatMapOperator<String, Tuple2<String, Long>> a1 = LineData.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {

            String words[] = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }

        })
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        UnsortedGrouping<Tuple2<String, Long>> a2 = a1.groupBy(0);
        AggregateOperator<Tuple2<String, Long>> a3 = a2.sum(1);

        a3.print();


    }

}