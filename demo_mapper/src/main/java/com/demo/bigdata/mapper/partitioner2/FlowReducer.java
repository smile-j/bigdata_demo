package com.demo.bigdata.mapper.partitioner2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean,Text, FlowBean> {

    private FlowBean flowBean = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        Long sumUp =0L;
        Long sumDown =0L;
        for (FlowBean value : values) {
            sumUp+=value.getUpFlow();
            sumDown+=value.getDownFlow();
        }
        flowBean.setUpFlow(sumUp).setDownFlow(sumDown);
        flowBean.setSumFlow();
        context.write(key,flowBean);
    }
}
