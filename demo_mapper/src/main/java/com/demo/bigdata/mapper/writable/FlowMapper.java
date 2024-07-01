package com.demo.bigdata.mapper.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text outK = new Text();
    private FlowBean outV = new FlowBean();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /**
         * value
         * 1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
         */
        String[] split = value.toString().split("\t");
        int len = split.length;
        String up = split[len-3];
        String down = split[len-2];
        outK.set(split[1]);
        outV.setUpFlow(Long.parseLong(up))
                .setDownFlow(Long.parseLong(down));
        outV.setSumFlow();
        context.write(outK, outV);
    }
}
