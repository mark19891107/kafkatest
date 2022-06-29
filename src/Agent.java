import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Agent {
	private Properties loadProps(String path) throws IOException {
		InputStream input = new FileInputStream(path);
		Properties prop = new Properties();
		prop.load(input);

		System.out.println("======= LOAD PROPERTIES START =======");
		System.out.println("KAFKA_SERVERS : " + prop.getProperty("KAFKA_SERVERS"));
		System.out.println("        TOPIC : " + prop.getProperty("TOPIC"));
		System.out.println("        EQPID : " + prop.getProperty("EQPID"));
		System.out.println("======= LOAD PROPERTIES END   =======\n\n");
		return prop;
	}

	private void run(Properties props) {
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty("KAFKA_SERVERS"));
		props.put(ConsumerConfig.GROUP_ID_CONFIG, props.getProperty("EQPID"));
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		System.out.println("======= AGENT STARTING        =======");
		
		final Consumer<Long, String> consumer = new KafkaConsumer<>(props);
		

		// Subscribe to the topic.
		consumer.subscribe(Arrays.asList(props.getProperty("TOPIC").split(",")));
		
		
		Thread mainThread = Thread.currentThread();
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
                System.out.println("======= CLOSING AGENT =======");
                consumer.wakeup();
                System.out.println("CONSUMER WAKEUP : OK");
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
		});


		System.out.println("======= AGENT WORKING         =======\n\n");
		
		try {
			while (true) {
				final ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
				consumerRecords.forEach(record -> {
					System.out.printf("Consumer Record:(%s, %s, %d, %d)\n", record.key(), record.value(),
							record.partition(), record.offset());
				});
				consumer.commitAsync();
			}
		} 
		catch (WakeupException e) {
		}
		catch (Exception e) {
            e.printStackTrace();
		} finally {
			consumer.close();
            System.out.println("CONSUMER CLOSE  : OK");
            System.out.println("=======  AGENT CLOSED  =======");
		}

	}

	public static void main(String[] args) throws IOException {
		if (args.length == 0) {
			System.out.println("USAGE: java -jar Agent.jar /path/to/config.properties");
		} else {
			Agent agent = new Agent();
			Properties props = agent.loadProps(args[0]);
			agent.run(props);
		}
	}

}
