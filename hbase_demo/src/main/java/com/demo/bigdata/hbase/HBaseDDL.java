/*
package com.demo.bigdata.hbase;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class HBaseDDL {

   public static Connection connection ;

    @Before
    public void before(){
        System.out.println("....init connection");
        connection = HBaseConnection.connection;

    }


    */
/**
     * 创建命名空间
     * @param nameSpace
     * @throws IOException
     *//*

    public static void createNameSpace(String nameSpace) throws IOException {

        */
/**
         * admin的连接是轻量级的，不是线程安全的，不推荐池化或者缓存这个连接
         *//*

        Admin admin = connection.getAdmin();
        NamespaceDescriptor descriptor = NamespaceDescriptor.create(nameSpace)
                .addConfiguration("user","uhadoop")
                .build();
        try {
            admin.createNamespace(descriptor);
        } catch (IOException e) {
            System.out.println("命名空间已存在");
            e.printStackTrace();
        }

        admin.close();
    }

    */
/**
     * 判断表是否存在
     * @param namespace
     * @param tableName
     * @return
     *//*

    public static boolean isTableExists(String namespace,String tableName) throws IOException {
        Admin admin = connection.getAdmin();

        boolean b = false;
        try {
            b = admin.tableExists(TableName.valueOf(namespace, tableName));
        } catch (IOException e) {
            System.out.println("。。。。判断异常");
            e.printStackTrace();
        }
        admin.close();
        return b;
    }

    */
/**
     * 创建表
     * @param namespace
     * @param tableName
     * @param columnFamilys
     *//*

    public static void createTable(String namespace,String tableName,String... columnFamilys) throws IOException {
        if(columnFamilys.length==0){
            System.out.println("必须有一个列族");
            return;
        }
        if(isTableExists(namespace,tableName)){
            return;
        };
        Admin admin = connection.getAdmin();
        List<ColumnFamilyDescriptor> columnFamilyDescriptorList = Arrays.stream(columnFamilys)
                .map(columnFamily ->
                        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily))
                                .setMaxVersions(3)
                                .build())
                .collect(Collectors.toList());
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace,tableName))
                .setColumnFamilies(columnFamilyDescriptorList)
                .build();
        try {
            admin.createTable(tableDescriptor);
        } catch (IOException e) {
            System.out.println("创建异常....");
            e.printStackTrace();
        }
        admin.close();
    }


    */
/**
     * 修改表格列族的版本
     * @param namespace
     * @param tableName
     * @param columnFamily
     * @param version
     *//*

    public static void modifyTable(String namespace,String tableName,String columnFamily,int version) throws IOException {
        Admin admin = connection.getAdmin();
        if(!isTableExists(namespace,tableName)){
            System.out.println("表格不存在");
            return;
        }

        TableDescriptor oldTableDescriptor = admin.getDescriptor(TableName.valueOf(namespace, tableName));
        TableDescriptor tableDescriptor = TableDescriptorBuilder
//                .newBuilder(TableName.valueOf(namespace, tableName))
                .newBuilder(oldTableDescriptor)
                .modifyColumnFamily(ColumnFamilyDescriptorBuilder
                        .newBuilder(Bytes.toBytes(columnFamily))
                        .setMaxVersions(version)
                        .build())
                .build();
        try {
            admin.modifyTable(tableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }
        admin.close();
    }

    public static boolean delTable(String namespace,String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        if(!isTableExists(namespace,tableName)){
            System.out.println("表格不存在");
            return false;
        }
        try {
            TableName tableName1 = TableName.valueOf(namespace, tableName);
            admin.disableTable(tableName1);
            admin.deleteTable(tableName1);
        } catch (IOException e) {
            System.out.println("删除异常");
            e.printStackTrace();
        }
        admin.close();
        return true;

    }

    @Test
    public void test() throws IOException {
//        createNameSpace("test_uhadoop");
//        boolean tableExists = isTableExists("bigdata", "person1");
//        System.out.println(tableExists);
//        createTable("test_uhadoop","student","info","msg");
//        modifyTable("test_uhadoop","student2","info",2);
        delTable("test_uhadoop","student");
        System.out.println("。。。。业务代码");
    }

    @After
    public void after() throws IOException {
        HBaseConnection.closeConn();
        System.out.println("连接已关闭");
    }

}
*/
