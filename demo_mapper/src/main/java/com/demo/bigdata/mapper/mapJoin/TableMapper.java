package com.demo.bigdata.mapper.mapJoin;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class TableMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Map<String, String> pdMap = new HashMap<>();
    private Text outV = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取缓存文件
        URI[] cacheFiles = context.getCacheFiles();
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(new Path(cacheFiles[0]));

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
        String line;
        while (StringUtils.isNotBlank(line = bufferedReader.readLine())) {
            String[] fileds = line.split("\t");
            pdMap.put(fileds[0], fileds[1]);
        }
        IOUtils.closeStream(bufferedReader);

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        //1001	01	1
        String pName = pdMap.get(split[1]);
        outV.set(split[0]+"\t"+pName+"\t"+split[1]);
        context.write(outV, NullWritable.get());

    }
}
