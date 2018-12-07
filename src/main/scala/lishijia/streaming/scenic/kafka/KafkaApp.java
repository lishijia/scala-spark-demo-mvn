package lishijia.streaming.scenic.kafka;

import com.alibaba.fastjson.JSONObject;

public class KafkaApp {

    public static void main(String args[]){
        KafkaSenderProcessor processor = new KafkaSenderProcessor();
        Order order = new Order();
        order.setScenic_code("s0001");
        order.setScenic_name("景区s0001");
        order.setChannel_name("销售渠道c001");
        order.setChannel_code("c001");
        order.setOrder_no("s000100000001");
        order.setPlace_time("2018-12-05 12:23:12");
        order.setSettle_price("110.00");
        order.setSettle_amount("110.00");
        order.setCertificate_no("430502xxxx06176212");
        order.setMobile_no("136xxxx6224");

        KafkaConfiguration configuration = new KafkaConfiguration();
        configuration.setHost("hadoop101:9092,hadoop102:9092,hadoop103:9092");

        processor.setKafkaConfiguration(configuration);
        processor.init();

        processor.send(JSONObject.toJSONString(order), "orderTopic");

        System.out.println("done");
    }

}
