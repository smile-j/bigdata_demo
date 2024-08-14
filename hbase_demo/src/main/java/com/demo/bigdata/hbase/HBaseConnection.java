/*
package com.demo.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class HBaseConnection {

    public static Connection connection = null;
    static {
        //1.创建连接
        try {
            //使用读取本地文件的形式添加参数
            connection = ConnectionFactory.createConnection();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //
    public static void closeConn() throws IOException {
        if(connection!=null){
            connection.close();
        }

    }

    public static void main(String[] args) throws IOException {

        System.out.println(connection);
        closeConn();

    }

    private static void singleConn() throws IOException {
        //1.创建配置连接对象
        Configuration config = new Configuration();
        //2.添加配置对象
        config.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        //3.同步连接
        Connection connection = ConnectionFactory.createConnection(config);
        //可以使用异步连接
//        CompletableFuture<AsyncConnection> asyncConnection = ConnectionFactory.createAsyncConnection(config);
        //4.使用连接
        System.out.println(connection);
        //5.回收
        connection.close();
    }

}
*/
