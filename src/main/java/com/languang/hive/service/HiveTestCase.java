package com.languang.hive.service;

import java.sql.*;
import org.apache.thrift.transport.TTransportException;

/**
 * Copyright: 版权所有 ( c ) 北京蓝光信息技术有限公司 2014。保留所有权利
 * 作者: 郭宁
 * 创建时间:2014/9/24.
 * 文件描述:
 * 修改描述：
 */
public class HiveTestCase {
    private static final String URLHIVE = "jdbc:hive://192.168.0.238:10000/default";
    private static Connection connection = null;

    public static void main(String[] args) throws  Exception {
        Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
        String dropSQL="drop table javabloger";
        String createSQL="create table javabloger (key int, value string)";
        //hive插入数据支持两种方式一种：load文件，令一种为从另一个表中查询进行插入（感觉这是个鸡肋）

        //hive是不支持insert into...values(....)这种操作的

        String insterSQL="LOAD DATA LOCAL INPATH '/home/h1/hive/examples/files/kv1.txt' OVERWRITE INTO TABLE javabloger";
        String querySQL="SELECT a.* FROM javabloger a";

        Connection con = DriverManager.getConnection(URLHIVE, "", "");
        Statement stmt = con.createStatement();
        stmt.executeQuery(dropSQL);  // 执行删除语句
        stmt.executeQuery(createSQL);  // 执行建表语句
        stmt.executeQuery(insterSQL);  // 执行插入语句
        ResultSet res = stmt.executeQuery(querySQL);   // 执行查询语句

        while (res.next()) {
            System.out.println("Result: key:"+res.getString(1) +"  –>  value:" +res.getString(2));
        }
    }
}
