package com.demo.bigdata.mapper.partitioner2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text,FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        String prePhone = text.toString().substring(0, 3);
        int partitioner;
        switch (prePhone){
            case "135":partitioner=0;break;
            case "136":partitioner=1;break;
            case "137":partitioner=2;break;
            case "138":partitioner=3;break;
            default:partitioner =4;
        }
        return partitioner;
    }
}
