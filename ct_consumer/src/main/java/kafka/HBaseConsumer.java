package kafka;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import utils.PropertiesUtil;

import java.util.Arrays;


/**
 * Created by xz on 2019/2/28.
 */
public class HBaseConsumer {

    public static void main(String[] args) {
        //创建配置对象
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<String,String>(PropertiesUtil.properties);
        //得到当前消费主题
        kafkaConsumer.subscribe(Arrays.asList(PropertiesUtil.getProperty("kafka.topic")));
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            for (ConsumerRecord record : records) {
                System.out.println(record.value());
            }
        }
    }
}
