package jacy.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class Consumer1 {

	private static final String ZOOKEEPER_NODES = "localhost:2181";
	private final ConsumerConnector consumer;
	private final String topic;
	private ExecutorService executor;

	public Consumer1(String topic) {
		consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());
		this.topic = topic;
	}

	public void shutdown() {
		if (consumer != null)
			consumer.shutdown();
		if (executor != null)
			executor.shutdown();
	}

	public void run(int a_numThreads) {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(a_numThreads));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

		executor = Executors.newFixedThreadPool(a_numThreads);

		int threadNumber = 0;
		for (final KafkaStream<byte[], byte[]> stream : streams) {
			executor.submit(new ConsumerTest(stream, threadNumber));
			threadNumber++;
		}
	}

	private static ConsumerConfig createConsumerConfig() {
		Properties props = new Properties();
		props.put("zookeeper.connect", ZOOKEEPER_NODES);
		props.put("group.id", "consumA");
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "smallest");

		return new ConsumerConfig(props);
	}

	public static void main(String[] args) {
		Consumer1 example = new Consumer1(Producer1.TOPIC);
		example.run(2);
//		example.shutdown();
	}

	class ConsumerTest implements Runnable {
		private KafkaStream<byte[], byte[]> m_stream;
		private int m_threadNumber;

		public ConsumerTest(KafkaStream<byte[], byte[]> a_stream, int a_threadNumber) {
			m_threadNumber = a_threadNumber;
			m_stream = a_stream;
		}

		public void run() {
			ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
			while (it.hasNext())
				System.out.println("Thread " + m_threadNumber + ": " + new String(it.next().message()));
			System.out.println("Shutting down Thread: " + m_threadNumber);
		}
	}
}