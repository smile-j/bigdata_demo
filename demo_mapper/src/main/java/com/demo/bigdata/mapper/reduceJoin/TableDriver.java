package com.demo.bigdata.mapper.reduceJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TableDriver {

    /**
     * join
     * @param args
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取job
        Configuration config = new Configuration();
        Job job = Job.getInstance(config);
        //2.设置jar包路径
        job.setJarByClass(TableDriver.class);
        //3.关联mapper和reducer
        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReduce.class);

        //4.设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        //5.设置最终输出的kv类型
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        //6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\bigdata\\input\\inputtable\\"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\bigdata\\output\\inputtable"));

        //7.提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
