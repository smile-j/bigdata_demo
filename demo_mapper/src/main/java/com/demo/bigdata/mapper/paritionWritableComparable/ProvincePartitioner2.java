package com.demo.bigdata.mapper.paritionWritableComparable;

import com.demo.bigdata.mapper.partitioner2.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner2 extends Partitioner<FlowBean,Text> {

    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {
        String prePhone = text.toString().substring(0,3);
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
