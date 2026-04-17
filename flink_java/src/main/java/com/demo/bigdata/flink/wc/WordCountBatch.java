package com.demo.bigdata.flink.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCountBatch {

    /**
     * flink版本：1.17
     * 批处理
     *
     */
    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.读取数据，从文件中读取
        DataSource<String> dataSource = env.readTextFile("datas/1.txt");

        //3.切分、转换（word,1）
        FlatMapOperator<String,Tuple2<String, Integer>> wordAndOne = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String w, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = w.split(" ");
                for (String word : words) {
                    Tuple2<String, Integer> wordTuple = Tuple2.of(word, 1);
                    out.collect(wordTuple);
                }
            }
        });

        //4.按照word分组
        UnsortedGrouping<Tuple2<String, Integer>> wordAndOneGroup = wordAndOne.groupBy(0);

        //5.各分组内聚合
        AggregateOperator<Tuple2<String, Integer>> sum = wordAndOneGroup.sum(1);
        //6.输出
        sum.print();


    }


}
