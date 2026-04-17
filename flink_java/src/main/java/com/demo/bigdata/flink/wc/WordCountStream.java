package com.demo.bigdata.flink.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountStream {

    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.读取数据，从文件中读取
        DataStreamSource<String> lineDS = env.readTextFile("datas/1.txt");

        SingleOutputStreamOperator<Tuple2<String,Integer>> wordToOne = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String w, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = w.split(" ");
                for (String word : words) {
                    Tuple2<String, Integer> wordTuple = Tuple2.of(word, 1);
                    out.collect(wordTuple);
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> wordAndOneKS = wordToOne.keyBy(tuple -> {
            return tuple.f0;
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> sumDs = wordAndOneKS.sum(1);

        sumDs.print();

        env.execute();


    }

}
