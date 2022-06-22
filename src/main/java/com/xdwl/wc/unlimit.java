package com.xdwl.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class unlimit {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> line1 = env.socketTextStream("hadoop102", 7777);

        SingleOutputStreamOperator<Tuple2<String, Long>> a4 = line1.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String words[] = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        })
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        KeyedStream<Tuple2<String, Long>, String> a5 = a4.keyBy(data -> data.f0);
        SingleOutputStreamOperator<Tuple2<String, Long>> a6 = a5.sum(1);

        a6.print();

        System.out.println("pushpush11");

        env.execute();


    }
}
