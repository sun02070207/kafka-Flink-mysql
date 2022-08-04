package com.sunyb.test1;/*
@author Serenity
@create 2022-07-28-21:54
*/

import Pojo.Person;
import com.sunyb.test1.Util.ProducerUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class WriteToKafka {
    public static void main(String[] args){
        int i = 0;
        boolean isFlag = true;
        while(isFlag) {
            try {
                //每秒写一条数据
                TimeUnit.SECONDS.sleep(1);
                writeToKafka();
                i++;
                if ( i >=4 ){
                    isFlag = false;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    public static void writeToKafka() {
        try {
            // 读取文件test.txt
            String srcFile = "D:\\test.txt";
            File file = new File(srcFile);
            InputStreamReader reader = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8);
            BufferedReader bufferedReader = new BufferedReader(reader);
            String line = "";

            // 创建一个生产者
            KafkaProducer<String, String> producer = ProducerUtil.createProducer("hadoop102:9092");
            while((line = bufferedReader.readLine()) != null) {
                // 发送消息到 Kafka，指定topic
                producer.send(new ProducerRecord<>("test", line));
                System.out.println(line);
            }
            //立即发送
            producer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
