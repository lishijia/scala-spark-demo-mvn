package lishijia.streaming.scenic.kafka;

import com.alibaba.fastjson.JSONObject;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

/**
 * 模拟发送消息
 */
public class KafkaApp {

    public static void main(String args[]) throws InterruptedException {

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd  HH:mm:ss");

        KafkaSenderProcessor processor = new KafkaSenderProcessor();
        KafkaConfiguration configuration = new KafkaConfiguration();
        configuration.setHost("master201:9092,slave202:9092,slave203:9092");
        processor.setKafkaConfiguration(configuration);
        processor.init();
        Random s = new Random();
        for(int i=0;i<10000;i++){
            Orders order = new Orders();
            int code = (s.nextInt(9)+1);
            order.setScenic_code("s000" + code);
            order.setScenic_name("景区s000" + code);
            order.setChannel_name("销售渠道c00" + code);
            order.setChannel_code("c00" + code);
            order.setOrder_no(System.currentTimeMillis()+"");
            order.setPlace_time(format.format(new Date()));
            order.setSettle_price("110.00");
            order.setSettle_amount("110.00");
            order.setCertificate_no("430502xxxx06176212");
            order.setMobile_no("136xxxx6224");
            //发送消息
            processor.send(JSONObject.toJSONString(order), "orderTopicV1");
            System.out.println("send mess i = " + i);
            Thread.sleep(1000);
        }
        System.out.println("done");
    }

}
