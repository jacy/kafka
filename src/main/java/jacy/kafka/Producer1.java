package jacy.kafka;

import java.util.Date;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class Producer1 {
	private static final String BROKER_ENDPOINTS = "localhost:9092";
	public static final String TOPIC = "mytopic";

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("metadata.broker.list", BROKER_ENDPOINTS);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);

		Producer<String, String> producer = new Producer<String, String>(config);

		Long start = System.currentTimeMillis();
		for (long nEvents = 0; nEvents < 5; nEvents++) {
			String msg = new Date().toString() + ", events >> " + nEvents;
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC, "" + nEvents, msg);
			producer.send(data);
		}
		Long end = System.currentTimeMillis();
		System.out.println(end - start);
		producer.close();
	}
}
