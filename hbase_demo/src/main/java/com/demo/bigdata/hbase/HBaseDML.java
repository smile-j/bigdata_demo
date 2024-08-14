/*
package com.demo.bigdata.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HBaseDML {

    public static Connection connection ;

    @Before
    public void before(){
        System.out.println("....init connection");
        connection = HBaseConnection.connection;

    }

    @Test
    public void testInsertCell() throws IOException {
        String nameSpace="bigdata",tableName="person",rowKey="1010"
        ,columnFamily="info",columnName="name",value="zhangsan";
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName),Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    @Test
    public void testQuery() throws IOException {
        String nameSpace="bigdata",tableName="person",rowKey="1003"
                ,columnFamily="info",columnName="name",value="zhangsan";
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));

        //一整行数据
        Get get = new Get(Bytes.toBytes(rowKey));
        //读取某一列
//        get.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName));

        get.readAllVersions();

        Result result = table.get(get);

        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            String v = new String(CellUtil.cloneValue(cell));
            System.out.println(v);
        }

        table.close();
    }

    @Test
    public void testScan() throws IOException {
        String nameSpace="bigdata",tableName="person",rowKey="1003"
                ,columnFamily="info",columnName="name",value="zhangsan"
                ,startrow="1001",stopRow="1010";
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));

        Scan scan = new Scan();
        //默认包含
        scan.withStartRow(Bytes.toBytes(startrow));
        //默认不包含
        scan.withStopRow(Bytes.toBytes(stopRow));
        //读取多行数据
        ResultScanner scanner = table.getScanner(scan);

        for (Result result:scanner){
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                String row = new String(CellUtil.cloneRow(cell));
                String family = new String(CellUtil.cloneFamily(cell));
                String qua = new String(CellUtil.cloneQualifier(cell));
                String v = new String(CellUtil.cloneValue(cell));
                System.out.print(row+"-"+family+"-"+qua+"-"+v+"\t");
            }
            System.out.println();
        }


        table.close();
    }

    */
/**
     * 带过滤的扫描
     * @throws IOException
     *//*

    @Test
    public void filterScan() throws IOException {
        String nameSpace="bigdata",tableName="person",rowKey="1003"
                ,columnFamily="info",columnName="name",value="zhangsan"
                ,startrow="1001",stopRow="1010";
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));

        Scan scan = new Scan();
        //默认包含
        scan.withStartRow(Bytes.toBytes(startrow));
        //默认不包含
        scan.withStopRow(Bytes.toBytes(stopRow));

        //可以添加多个过滤
        FilterList filterList = new FilterList();
        */
/**
         * 过滤器2种
         *//*

        //1.结果只保留当前列的数据
        ColumnValueFilter columnValueFilter = new ColumnValueFilter(
                //列族
                Bytes.toBytes(columnFamily)
                //列名字
                , Bytes.toBytes(columnName)
                //比较关系
                , CompareOperator.EQUAL
                , Bytes.toBytes(value));
        //2.结果保留整行数据
        //结果同时会保留没有当前列的数据
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                //列族
                Bytes.toBytes(columnFamily)
                //列名字
                , Bytes.toBytes(columnName)
                //比较关系
                , CompareOperator.EQUAL
                , Bytes.toBytes(value));

        filterList.addFilter(singleColumnValueFilter);

        scan.setFilter(filterList);

        //读取多行数据
        ResultScanner scanner = table.getScanner(scan);

        for (Result result:scanner){
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                String row = new String(CellUtil.cloneRow(cell));
                String family = new String(CellUtil.cloneFamily(cell));
                String qua = new String(CellUtil.cloneQualifier(cell));
                String v = new String(CellUtil.cloneValue(cell));
                System.out.print(row+"-"+family+"-"+qua+"-"+v+"\t");
            }
            System.out.println();
        }


        table.close();
    }

    */
/**
     * 删除一行种的一列
     *//*

    @Test
    public void testDelColumn() throws IOException {
        String nameSpace="bigdata",tableName="person",rowKey="1003"
                ,columnFamily="info",columnName="name",value="zhangsan"
                ,startrow="1001",stopRow="1010";
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));

        Delete delete = new Delete(Bytes.toBytes(rowKey));

        //删除一个版本
        delete.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName));
        //删除所有版本
//        delete.addColumns(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName));

        table.delete(delete);
        table.close();
    }

    @After
    public void after() throws IOException {
        HBaseConnection.closeConn();
        System.out.println("连接已关闭");
    }
}
*/
