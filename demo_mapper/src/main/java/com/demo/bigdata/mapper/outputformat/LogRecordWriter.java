package com.demo.bigdata.mapper.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;


public class LogRecordWriter extends RecordWriter<Text, NullWritable> {


    private  FSDataOutputStream guiguOut;
    private  FSDataOutputStream otherOut;

    public  LogRecordWriter(TaskAttemptContext job) {
        //创建俩个输出流
        try {
            FileSystem fileSystem = FileSystem.get(job.getConfiguration());
             guiguOut = fileSystem.create(new Path("D:\\bigdata\\atguigu.log"));
             otherOut = fileSystem.create(new Path("D:\\bigdata\\other.log"));
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String log = key.toString();
        if(log.contains("atguigu")){
            guiguOut.writeBytes(log+"\n");
        }else {
            otherOut.writeBytes(log+"\n");
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {

        //关闭流
        IOUtils.closeStream(guiguOut);
        IOUtils.closeStream(otherOut);

    }
}
