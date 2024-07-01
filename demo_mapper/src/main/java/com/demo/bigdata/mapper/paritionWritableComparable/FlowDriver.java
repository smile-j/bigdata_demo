package com.demo.bigdata.mapper.paritionWritableComparable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取job
        Configuration config = new Configuration();
        Job job = Job.getInstance(config);
        //2.设置jar
        job.setJarByClass(FlowDriver.class);

        //3.关联mapper,reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //4.设置mapper 输出的key和value类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        //5.设置最终输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        job.setPartitionerClass(ProvincePartitioner2.class);
        job.setNumReduceTasks(5);

        //6.设置数据的输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\bigdata\\input\\phone_data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\bigdata\\output888"));

        //7.提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }


}
