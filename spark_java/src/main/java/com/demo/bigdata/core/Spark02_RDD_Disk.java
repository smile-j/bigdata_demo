package com.demo.bigdata.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Spark02_RDD_Disk {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        //        conf.setMaster("local[2]");//多线程
        //单线程
        conf.setMaster("local");
        conf.setAppName("wordName");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = jsc.textFile("datas/1.txt");
         rdd.collect().forEach(System.out::println);

        jsc.close();
    }

}
