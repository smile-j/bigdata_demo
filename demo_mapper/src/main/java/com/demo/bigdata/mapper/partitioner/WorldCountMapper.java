package com.demo.bigdata.mapper.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * KEYIN, mapper阶段输入的key的类型：LongWritable
 * VALUEIN,mapper阶段输入的value的类型:Text
 * KEYOUT, mapper阶段输出的key的类型，Text
 * VALUEOUT mapper阶段输出的value的类型，IntWritable
 */
public class WorldCountMapper extends Mapper<LongWritable,Text,Text, IntWritable> {
    Logger logger  = LoggerFactory.getLogger(WorldCountMapper.class);

    private Text outK = new Text();
    private IntWritable outV = new IntWritable(1);


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取一行
        String line = value.toString();
        //2.切割
        String[] words = line.split(" ");
        //3.循环写出
        for (String word : words) {
            //封装
            outK.set(word);
            //输出
            context.write(outK,outV);
        }
    }
}
