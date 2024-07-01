package com.demo.bigdata.mapper.paritionWritableComparable;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@NoArgsConstructor
@Data
@Accessors(chain = true)
public class FlowBean implements WritableComparable<FlowBean> {

    private Long upFlow;
    private Long downFlow;
    private Long sumFlow;

    public void setSumFlow(){
        this.sumFlow = this.downFlow+this.upFlow;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();
    }

    @Override
    public String toString(){
        return this.upFlow+"\t"+this.downFlow+"\t"+this.sumFlow;
    }

    @Override
    public int compareTo(FlowBean flowBean) {
        //总流量倒序
        if(this.sumFlow>flowBean.getSumFlow()){
            return -1;

        }else if(this.sumFlow<flowBean.getSumFlow()){
            return 1;
        }else {
            //上行流量的正序
            if(this.upFlow>flowBean.getUpFlow()){
                return 1;
            }else if(this.upFlow<flowBean.getUpFlow()){
                return -1;
            }else {
                return 0;
            }
        }
    }
}
