package com.demo.bigdata.yarn.word;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;

public class WordCountDriver {

    private static Tool tool;

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        switch (args[0]){
            case "wordcount":
               tool  = new WordCount();
               break;
            default:
                throw  new RuntimeException("参数异常"+args[0]);
        }

        int run = ToolRunner.run(conf, tool, Arrays.copyOfRange(args, 1, args.length));

        System.exit(run);

    }


}
