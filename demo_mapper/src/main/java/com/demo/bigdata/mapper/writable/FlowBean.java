package com.demo.bigdata.mapper.writable;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@NoArgsConstructor
@Data
@Accessors(chain = true)
public class FlowBean implements Writable {

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
}
