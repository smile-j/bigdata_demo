package com.demo.bigdata.mapper.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WebLogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        boolean res = parseLog(line, context);
        if (!res) {
            return;
        }
        context.write(value, NullWritable.get());
    }

    private boolean parseLog(String line, Context context) {

        String[] fields = line.split(" ");
        //判断日志的长度是否大于11
        if (fields.length > 11) {
            return true;
        }

        return false;
    }


}
