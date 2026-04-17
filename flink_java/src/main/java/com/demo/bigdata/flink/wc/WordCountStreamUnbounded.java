package com.demo.bigdata.flink.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountStreamUnbounded {
    /**
     * 无界
     * 模拟发送数据 nc -l -p 9999
     */
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //IDEA 运行时，也可以看到webui,一般用于本地测试  http://localhost:8081/
        //需要引入一个依赖 flink-runtime-web
        //在idea运行，不指定并行度，默认就是电脑的线程数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//        env.setParallelism(2);
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        Integer port = parameterTool.getInt("port");
        System.out.println("host:"+host+"port:"+port);

        DataStreamSource<String> socketDS = env.socketTextStream(host, port);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = socketDS.flatMap(
                (String word, Collector<Tuple2<String, Integer>> out) -> {
                    for (String w : word.split(" ")) {
                        out.collect(Tuple2.of(w, 1));
                    }
                }
                )
                //设置当前算子的并行度
                .setParallelism(2)
                .returns(Types.TUPLE(Types.STRING,Types.INT)).keyBy(value -> value.f0)
                .sum(1);
        sum.print();
        env.execute();
    }

    /**
     * 并行度优先级：算子 > env > 提交指定
     *
     */
}
