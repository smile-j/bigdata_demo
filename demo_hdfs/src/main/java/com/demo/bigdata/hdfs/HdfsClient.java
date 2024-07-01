package com.demo.bigdata.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class HdfsClient {

    //Distributed FileSystem
    FileSystem fs = null;

    @Before
    public void init()throws URISyntaxException, IOException, InterruptedException{
        //连接的集群nn地址
        URI uri = new URI("hdfs://hadoop102:8020");
        //创建一个配置文件
        Configuration configuration = new Configuration();

        configuration.set("dfs.replication","2");

        //用户
        String user ="uhadoop";
        //1.获取到了客户端对象
        fs = FileSystem.get(uri, configuration,user);
    }

    @After
    public void close()throws URISyntaxException, IOException, InterruptedException{
        //3.关闭资源
        fs.close();
    }


    /**
     * 源码
     */
    @Test
    public void readSource() throws IOException {
        FSDataOutputStream fs = this.fs.create(new Path("/input"));
        fs.writeBytes("hello world!!!");
    }

    //创建目录
    @Test
    public void testmkdir() throws IOException {
        //2.创建一个文件夹
        fs.mkdirs(new Path("/xiyou/huaguoshan"));

    }

    //上传

    /**
     * 参数优先级
     * hdfs-default.xml => hdfs-site.xml =>项目资源目录下的配置文件 => 代码里的配置
     */
    @Test
    public void testPut() throws IOException {
        /**
         * boolean delSrc, boolean overwrite, Path src, Path dst
         * 参数1：删除原数据
         * 参数2：是否允许覆盖
         * 参数3：原始数据路径
         * 参数4：目的地路径
         */
        fs.copyFromLocalFile(false,true,new Path("D:\\FFOutput\\sunwukong.txt"),new Path("hdfs://hadoop102/xiyou/huaguoshan"));
    }

    //文件下载
    @Test
    public void testGet() throws IOException {
        /**
         * boolean delSrc, Path src, Path dst, boolean useRawLocalFileSystem
         * 参数1：是否删除原文件
         * 参数2：原文件路径
         * 参数3：目标地址路径
         * 参数4:是否开启校验
         */
        fs.copyToLocalFile(false,new Path("hdfs://hadoop102/xiyou/huaguoshan"),new Path("D://FFOutput//down_sunwukong"),true);

    }

    //删除
    @Test
    public void testDel() throws IOException {
        /**
         * Path var1, boolean var2
         * 参数1：要删除的路径
         * 参数2：是否递归删除
         */
        //删除文件
        boolean delete = fs.delete(new Path("/demo_out"),false);

        //删除目录

        fs.delete(new Path("/xiyou"),false);

        //删除非空目录
        fs.delete(new Path("xiyou"),true);

    }

    //文件更名和移动
    @Test
    public void testMv() throws IOException {
        /**
         * Path var1, Path var2
         * 参数一：原文件目录
         * 参数二:目标文件路径
         */
        //对文件的名称修改
//        fs.rename(new Path("/demo_input/word.txt"),new Path("/demo_input/ss.txt"));

        //对文件移动并修改名称
        fs.rename(new Path("/word.txt"),new Path("/demo_input/word.txt"));

        //目录的更名
//        fs.rename(new Path("/demo_out"),new Path("/demo_out2"));
    }

    //获取文件详细信息
    @Test
    public void fileDetail() throws IOException {
        RemoteIterator<LocatedFileStatus> listFile = fs.listFiles(new Path("/"), true);
        while (listFile.hasNext()){
            LocatedFileStatus next = listFile.next();
            System.out.println("==============="+next.getPath()+"===============");
            System.out.println(""+next.getPermission());
            System.out.println(next.getOwner());
            System.out.println(next.getGroup());
            System.out.println(next.getLen());
            System.out.println(next.getModificationTime());
            System.out.println(next.getReplication());
            System.out.println(next.getBlockSize());
            System.out.println(next.getPath().getName());

            //获取块信息
            BlockLocation[] blockLocations = next.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
    }

    /**
     * 判断文件夹 还是 文件
     */
    @Test
    public void testFile() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            if(fileStatus.isFile()){
                System.out.println("文件："+fileStatus.getPath().getName());
            }else {
                System.out.println("目录："+fileStatus.getPath().getName());
            }
        }

    }

}
