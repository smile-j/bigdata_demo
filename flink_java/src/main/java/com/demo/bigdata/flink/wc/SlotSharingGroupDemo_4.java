package com.demo.bigdata.flink.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SlotSharingGroupDemo_4 {
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

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        Integer port = parameterTool.getInt("port");
        System.out.println("host:"+host+"port:"+port);

        env.socketTextStream(host, port)
                .flatMap(
                (String word, Collector<Tuple2<String, Integer>> out) -> {
                    for (String w : word.split(" ")) {
                        out.collect(Tuple2.of(w, 1));
                    }
                }
                )
                .returns(Types.TUPLE(Types.STRING,Types.INT)).keyBy(value -> value.f0)
                .sum(1).print();
        env.execute();
    }

    /**
     * 1. slot特点：
     *    1) 均分隔离内存，不隔离CPU
     *    2) 可以共享
     *          同一个job中，不同算子的任务 可以共享 同一个slot,同时运行的
     *          前提是，属于同一个slot共享组，默认都是“否”
    *
     * 2. slot数量 与 并行度的关系
     *    1）slot是一种静态的概念，表示最大的并发上限
     *         并行度是一种动态的概念，表示实际运行占用了几个
     *    2）要求：slot数量 >= job并行度（算子最大并行度），job才能运行
     *
     */

}
