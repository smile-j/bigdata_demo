package com.demo.bigdata.mapper.mapJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class TableDriver {

    /**
     * join
     *
     * @param args
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration config = new Configuration();
        Job job = Job.getInstance(config);
        job.setJarByClass(TableDriver.class);
        job.setMapperClass(TableMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.addCacheFile(new URI("file:///D:/bigdata/input/inputtable/pd.txt"));
        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job,new Path("D:\\bigdata\\input\\inputtable2"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\bigdata\\output\\inputtable2"));

        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);
    }
}
