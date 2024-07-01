package com.demo.bigdata.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

public class NNServier implements RPCProtocol{

    public static void main(String[] args) throws IOException {

        RPC.Server server = new RPC.Builder(new Configuration())
                .setBindAddress("localhost")
                .setPort(8888)
                .setProtocol(RPCProtocol.class)
                .setInstance(new NNServier())
                .build();

        System.out.println("服务器开始工作");
        server.start();

    }

    @Override
    public void mkdirs(String path) {
        System.out.println("服务器接收到了客户端请求"+path);
    }
}
