package com.demo.bigdata.mapper.paritionWritableComparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    private Text outV = new Text();
    private FlowBean outK = new FlowBean();


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /**
         * value
         * 3736230513	100	200   300
         */
        String line = value.toString();

        String[] split = line.split("\t");

        outV.set(split[0]);
        outK.setUpFlow(Long.parseLong(split[1]))
                .setDownFlow(Long.parseLong(split[2]))
                .setSumFlow();
        context.write(outK,outV);
    }
}
