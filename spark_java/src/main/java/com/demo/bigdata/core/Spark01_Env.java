package com.demo.bigdata.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Spark01_Env {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        //        conf.setMaster("local[2]");//多线程
        conf.setMaster("local");//单线程
        conf.setAppName("wordName");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        jsc.close();
    }
}
