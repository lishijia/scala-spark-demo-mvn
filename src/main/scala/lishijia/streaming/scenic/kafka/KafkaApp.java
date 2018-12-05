package lishijia.streaming.scenic.kafka;

import com.alibaba.fastjson.JSONObject;

public class KafkaApp {

    public static void mian(String args[]){
        KafkaSenderProcessor processor = new KafkaSenderProcessor();
        Order order = new Order();

        KafkaConfiguration configuration = new KafkaConfiguration();
        configuration.setHost("master201");

        processor.setKafkaConfiguration(configuration);
        processor.init();

        processor.send(JSONObject.toJSONString(order), "orderTopic");

    }

}
