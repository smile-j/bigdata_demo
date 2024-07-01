package com.demo.bigdata.mapper.reduceJoin;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@NoArgsConstructor
public class TableBean implements Writable {

    /**
     * 订单id
     */
    private String id;

    /**
     * 商品id
     */
    private String pid;

    /**
     * 商品数量
     */
    private int amount;
    /**
     * 商品名称
     */
    private String pname;
    /**
     * 标记是什么表 order,pd
     */
    private String flag;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(pid);
        out.writeInt(amount);
        out.writeUTF(pname);
        out.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        this.id = input.readUTF();
        this.pid = input.readUTF();
        this.amount =input.readInt();
        this.pname = input.readUTF();
        this.flag = input.readUTF();
    }

    @Override
    public String toString(){
        return id+"\t"+pname+"\t"+amount;
    }
}
