package com.demo.bigdata.mapper.yasuo;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.io.compress.bzip2.Bzip2Compressor;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取job
        Configuration config = new Configuration();
        config.setBoolean("mapreduce.map.output.compress",true);
//        config.setClass("mapreduce.map.output.compress.codes", BZip2Codec.class, CompressionCodec.class);
        config.setClass("mapreduce.map.output.compress.codes", SnappyCodec.class, CompressionCodec.class);
        Job job = Job.getInstance(config);
        //2.设置jar包路径
        job.setJarByClass(WordCountDriver.class);
        //3.关联mapper和reducer
        job.setMapperClass(WorldCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //4.设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5.设置最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\bigdata\\input\\hello"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\bigdata\\output4"));

//        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
//        FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);

        //7.提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
