package com.demo.bigdata.hbase;

import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

public class PhoenixTest {

    private Connection connection;

    @Before
    public void connection() throws SQLException {

        String url="jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
        //配置对象，没有用户名/密码
        Properties properties = new Properties();

        connection = DriverManager.getConnection(url, properties);
        PreparedStatement preparedStatement = connection.prepareStatement("select * from bigdata.test");
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()){
            System.out.println(resultSet.getString("name"));
            System.out.println(resultSet.getString("address"));
        }
        connection.close();
    }

    @Test
    public void test1(){
        System.out.println(11);
    }

}
