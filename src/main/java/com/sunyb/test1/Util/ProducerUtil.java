package com.sunyb.test1.Util;/*
@author Serenity
@create 2022-07-28-22:06
*/

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerUtil {
    public static KafkaProducer<String, String> createProducer(String bootstrapServers){
        Properties props = new Properties();
        //kafka服务器地址版本
        props.put("bootstrap.servers", bootstrapServers);
        //设置数据key和value的序列化处理类
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);
        props.put("zookeeper.connect", "hadoop102:2181");
        return new KafkaProducer<>(props);
    }
}
