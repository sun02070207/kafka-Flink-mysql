package com.sunyb.test1;/*
@author Serenity
@create 2022-07-29-16:24
*/

import Pojo.Person;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.LinkedList;
import java.util.Properties;
@Slf4j
public class FlinkToKafka {
    private static final Properties prop = new Properties();
    private static final String BOOTSTRAP = "hadoop102:9092";
    private static final String ZOOKEEPER = "hadoop102:2181";

    static {
        prop.put("bootstrap.servers", BOOTSTRAP);
        prop.put("zookeeper.connect", ZOOKEEPER);
        prop.put("key.deserializer", StringDeserializer.class);
        prop.put("value.deserializer", StringDeserializer.class);
    }

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);

        //代码这块有问题
//        DataStreamSource<WriteToKafka> writeToKafkaDataStreamSource = env.fromElements();
//        writeToKafkaDataStreamSource.print();

        //添加topic
        LinkedList<String> topics = new LinkedList<>();
        topics.add("test");

        //创建对象
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topics, new SimpleStringSchema(), prop);

        consumer.setStartFromLatest();

        DataStream<String> stream = env.addSource(consumer);
        env.enableCheckpointing(5000);

        //获取kafka的消息
        log.info("从kafka接收到的消息");
        stream.print("从kafka接收到的消息");

        //写一个输出流算子
        SingleOutputStreamOperator<Person> energyMySQL = stream.map(new MyMapFunction());
        energyMySQL.print();
        //连接mysql数据库，执行相应的sql语句
        log.info("获取数据库连接");
        energyMySQL.addSink(new WriteMysqlSink());

        try {
            env.execute("flink parsing to mysql job");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    //清洗方法
    private static class MyMapFunction implements MapFunction<String, Person> {
        @Override
        public Person map(String value) throws Exception {
            String[] splits = value.split("\\|\\|");
            if (splits[0].equals("11")){
                splits[0] = "111";
            }else if (splits[0].equals("22")){
                splits[0] = "222";
            }
            Person person = new Person(Integer.valueOf(splits[0]), splits[1]);
            return person;
        }
    }
}
