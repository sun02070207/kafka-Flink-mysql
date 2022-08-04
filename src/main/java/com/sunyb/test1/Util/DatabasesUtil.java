package com.sunyb.test1.Util;/*
@author Serenity
@create 2022-07-31-21:49
*/

import com.alibaba.druid.pool.DruidDataSource;
import java.sql.Connection;

public class DatabasesUtil {
    private static DruidDataSource dataSource;

    public static Connection getConnection() throws Exception {
        dataSource = new DruidDataSource();
//        dataSource.setDriverClassName("com.mysql.jdbc.Driver"); //弃用的驱动
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
//        dataSource.setUrl("jdbc:mysql://hadoop102:3306/test?useUnicode=true&characterEncoding=utf-8");
        dataSource.setUrl("jdbc:mysql://localhost:3306/test?serverTimezone=GMT");
        dataSource.setUsername("root");
//        dataSource.setPassword("123456");
        dataSource.setPassword("abc123");
        return  dataSource.getConnection();
    }
}
