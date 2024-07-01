package com.demo.bigdata.mapper.mapjoin2;

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

        job.addCacheFile(new URI(args[0]));
        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job,new Path(args[1]));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));

        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);
    }
}
