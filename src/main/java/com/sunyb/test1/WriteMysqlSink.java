package com.sunyb.test1;/*
@author Serenity
@create 2022-07-31-21:56
*/

import Pojo.Person;
import com.sunyb.test1.Util.DatabasesUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import lombok.extern.slf4j.Slf4j;
import java.sql.Connection;
import java.sql.PreparedStatement;

@Slf4j
public class WriteMysqlSink extends RichSinkFunction<Person> {
    private Connection connection = null;
    private PreparedStatement ps = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        log.info("获取数据库连接");
        //连接数据库
        connection = DatabasesUtil.getConnection();
        //关闭自动提交
        connection.setAutoCommit(false);
    }

    @Override
    public void invoke(Person value, Context ctx) throws Exception {
        // 获取发送过来的结果
        if (value == null){
            log.info("无法获取数据");
            return ;
        }
        String sql = "INSERT INTO employee(id, name) VALUES (?, ?);";
        ps = connection.prepareStatement(sql);
        ps.setInt(1, value.id);
        ps.setString(2, value.name);
        ps.execute();
        connection.commit();

        log.info("成功写入Mysql");

    }

    @Override
    public void close() throws Exception {
        //关闭并释放资源，从创建到销毁只会执行一次
        if(connection != null) {
            connection.close();
        }

        if(ps != null) {
            ps.close();
        }
    }
}
