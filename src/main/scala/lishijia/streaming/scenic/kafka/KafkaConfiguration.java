package lishijia.streaming.scenic.kafka;

public class KafkaConfiguration {

	private String host;

	private String keySerializer = "org.apache.kafka.common.serialization.StringSerializer";

	private String valueSerializer = "org.apache.kafka.common.serialization.StringSerializer";

	private String keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";

	private String valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";

	public String getHost() {

		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getKeySerializer() {
		return keySerializer;
	}

	public void setKeySerializer(String keySerializer) {
		this.keySerializer = keySerializer;
	}

	public String getValueSerializer() {
		return valueSerializer;
	}

	public void setValueSerializer(String valueSerializer) {
		this.valueSerializer = valueSerializer;
	}

	public String getFullIp() {
		return host;
	}

	public String getKeyDeserializer() {
		return keyDeserializer;
	}

	public void setKeyDeserializer(String keyDeserializer) {
		this.keyDeserializer = keyDeserializer;
	}

	public String getValueDeserializer() {
		return valueDeserializer;
	}

	public void setValueDeserializer(String valueDeserializer) {
		this.valueDeserializer = valueDeserializer;
	}

}
