package lishijia.streaming.scenic.kafka;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaSenderProcessor {

    private Logger logger = LoggerFactory.getLogger(KafkaSenderProcessor.class);

    private KafkaConfiguration kafkaConfiguration;

    private Producer<String, String> kafkaProducer;

    public boolean send(String value, String topic) {

        try {
            ProducerRecord<String, String> msg = new ProducerRecord<String, String>(topic, null, value);
            Future<RecordMetadata> f = kafkaProducer.send(msg);
            RecordMetadata resp = f.get();
            logger.info(" send message topic: " + topic + ", offset : " + resp.offset());
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage(), e);
            return false;
        }

        return true;
    }

    public void init() {
        Properties props = new Properties();
        long begin = System.currentTimeMillis();
        props.put("metadata.broker.list", kafkaConfiguration.getFullIp());
        props.put("key.serializer", kafkaConfiguration.getKeySerializer());
        props.put("value.serializer", kafkaConfiguration.getValueSerializer());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfiguration.getFullIp());
        logger.info(" kafka sender processor init ... ");
        kafkaProducer = new KafkaProducer<String, String>(props);
        logger.info(" kafka sender processor init complete, time: " + (System.currentTimeMillis() - begin));
    }


    public void setKafkaConfiguration(KafkaConfiguration kafkaConfiguration) {
        this.kafkaConfiguration = kafkaConfiguration;
    }


}
