/**
 * This file was automatically generated by the Mule Development Kit
 */
package net.taptech.kafka.mule.connector;

import com.google.common.util.concurrent.MoreExecutors;
import net.taptech.kafka.mule.connector.config.ConnectorConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.mule.api.MuleException;
import org.mule.api.annotations.*;
import org.mule.api.annotations.lifecycle.Start;
import org.mule.api.annotations.lifecycle.Stop;
import org.mule.api.annotations.param.Optional;
import org.mule.api.callback.SourceCallback;
import org.mule.api.endpoint.EndpointException;
import org.mule.api.transport.ConnectorException;
import org.mule.config.i18n.MessageFactory;
import org.mule.util.concurrent.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.ResourceLoader;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
@Connector(name="kafka", friendlyName="Kafka")
public class KafkaConnector {
	private static Logger logger = LoggerFactory.getLogger(KafkaConnector.class);
	
	@Config
	ConnectorConfig config;

	private Properties consumerProperties = new Properties();
	private Properties producerProperties = new Properties();
	private ExecutorService producerPool;
	private ExecutorService consumerPool;
	private static int connectorCount = 0;
	private ResourceLoader resourceLoader = new DefaultResourceLoader();

	private static final int DEFAULT_POOL_SIZE = 5;
	private static final long DEFAULT_THREAD_KEEP_ALIVE_TIME = 60L;

	static class KafkaConnectorNamedThreadFactory implements ThreadFactory {
		private static final AtomicInteger poolNumber = new AtomicInteger(1);
		private final ThreadGroup group;
		private final AtomicInteger threadNumber = new AtomicInteger(1);
		private final String namePrefix;

		KafkaConnectorNamedThreadFactory(String name) {
			SecurityManager s = System.getSecurityManager();
			group = (s != null) ? s.getThreadGroup() :
					Thread.currentThread().getThreadGroup();
			namePrefix = name;
		}

		public Thread newThread(Runnable r) {
			Thread t = new Thread(group, r,
					namePrefix + "-"+threadNumber.getAndIncrement(),
					0);
			if (t.isDaemon())
				t.setDaemon(false);
			if (t.getPriority() != Thread.NORM_PRIORITY)
				t.setPriority(Thread.NORM_PRIORITY);
			return t;
		}
	}
	
	@Start
	public void initialize() throws Exception{
		logger.info("KafkaConnector initialize called with count "+ ++connectorCount);
		consumerProperties.load(resourceLoader.getResource(config.getConsumerPropertiesFile()).getInputStream());
		producerProperties.load(resourceLoader.getResource(config.getProducerPropertiesFile()).getInputStream());
		int producerThreads = determineThreads(producerProperties.getProperty(KafkaConnectorConstants.PRODUCER_THREADS));
		int consumerThreads = determineThreads(consumerProperties.getProperty(KafkaConnectorConstants.CONSUMER_THREADS));

		producerPool = MoreExecutors.getExitingExecutorService(new ThreadPoolExecutor(producerThreads, Integer.MAX_VALUE,
						DEFAULT_THREAD_KEEP_ALIVE_TIME, TimeUnit.SECONDS,
						new SynchronousQueue<Runnable>(), new KafkaConnectorNamedThreadFactory("ProducerPool")),
				100, TimeUnit.MILLISECONDS);
		/*
		producerPool = new ThreadPoolExecutor(producerThreads, Integer.MAX_VALUE,
				DEFAULT_THREAD_KEEP_ALIVE_TIME, TimeUnit.SECONDS,
				new SynchronousQueue<Runnable>(), new KafkaConnectorNamedThreadFactory("ProducerPool"));
				*/
		consumerPool =  MoreExecutors.getExitingExecutorService(new ThreadPoolExecutor(consumerThreads, Integer.MAX_VALUE,
						DEFAULT_THREAD_KEEP_ALIVE_TIME, TimeUnit.SECONDS,
						new SynchronousQueue<Runnable>(), new KafkaConnectorNamedThreadFactory("ConsumerPool")),
				100, TimeUnit.MILLISECONDS);
	}

	private int determineThreads(String property) {

		int threads = DEFAULT_POOL_SIZE;
		if (null != property){
			try{
				 threads = Integer.valueOf(property);
			} catch( NumberFormatException nfe){
				StringBuilder sb = new StringBuilder("Error converting ");
				sb.append(property);
				sb.append("to int value: ");
				sb.append(nfe.getMessage());
				sb.append(" Using default value of ").append(DEFAULT_POOL_SIZE);
				sb.append(" for pool size.");
				logger.warn(sb.toString());
			}
		}
		return threads;
	}

	public Properties getConsumerProperties() {
		return consumerProperties;
	}

	public Properties getProducerProperties() {
		return producerProperties;
	}

	public ExecutorService getProducerPool() {
		return producerPool;
	}

	public ExecutorService getConsumerPool() {
		return consumerPool;
	}

	@Stop
	public void shutdownAndAwaitTermination() {
		ExecutorService [] servicesArray = {getProducerPool(), getConsumerPool()};
		for (ExecutorService pool: Arrays.asList(servicesArray)){
			pool.shutdown(); // Disable new tasks from being submitted
			try {
				// Wait a while for existing tasks to terminate
				if (!pool.awaitTermination(DEFAULT_THREAD_KEEP_ALIVE_TIME, TimeUnit.SECONDS)) {
					pool.shutdownNow(); // Cancel currently executing tasks
					// Wait a while for tasks to respond to being cancelled
					if (!pool.awaitTermination(DEFAULT_THREAD_KEEP_ALIVE_TIME, TimeUnit.SECONDS))
						System.err.println("Pool did not terminate");
				}
			} catch (InterruptedException ie) {
				// (Re-)Cancel if current thread also interrupted
				pool.shutdownNow();
				// Preserve interrupt status
				Thread.currentThread().interrupt();
			}
		}
	}

	public ConnectorConfig getConfig() {
		return config;
	}
	public void setConfig(ConnectorConfig config) {
		this.config = config;
	}

	public static final Integer DEFAULT_DELAY = 1000;

	private Properties copyProperties(Properties properties){
		Properties newProperties = new Properties();
		newProperties = (Properties) properties.clone();
		return newProperties;
	}

	@Source(name = "Consumer", friendlyName = "Consumer")
	public void consumer(SourceCallback callback, @Optional String topic, @Optional String propertyFileOverrides) {
		logger.info("consumer Creating simpleConsumer with propertyFileOverrides {}",propertyFileOverrides);
		Integer delay = DEFAULT_DELAY;
		String [] topicsArray = consumerProperties.getProperty(KafkaConnectorConstants.CONSUMER_TOPICS).split(",");
		Properties properties = copyProperties(getConsumerProperties());
		if (null != topic){
			properties.put(KafkaConnectorConstants.CONSUMER_TOPICS, topic);
		}
		KafkaConsumerRunner runner = new KafkaConsumerRunner(properties, callback);
		logger.debug("consumer Subscribing to topics {} with properties",topicsArray,properties);
		consumerPool.submit(runner);
	}


	@Processor(name = "Seek", friendlyName = "Seek")
	public List<ConsumerRecord<?, ?>> seek(int partition, String topic, int offset, @Optional String propertyFileOverrides) throws ExecutionException, InterruptedException {
		logger.info("seek Creating simpleConsumer with partition {}, topic {}, offset {},  propertyFileOverrides {}",new Object[]{partition, topic, offset, propertyFileOverrides});
		Integer delay = DEFAULT_DELAY;

		Properties properties = copyProperties(getConsumerProperties());
		properties.put(KafkaConnectorConstants.CONSUMER_PARTITION, new Integer(partition));
        properties.put(KafkaConnectorConstants.CONSUMER_OFFSET, new Integer(offset));
        properties.put(KafkaConnectorConstants.CONSUMER_TOPIC, topic);
        String [] topicsArray = properties.getProperty(KafkaConnectorConstants.CONSUMER_TOPICS).split(",");
		if (null != topic){
			properties.put(KafkaConnectorConstants.CONSUMER_TOPICS, topic);
		}
		KafkaConsumerOffsetRunner runner = new KafkaConsumerOffsetRunner(properties);
		logger.debug("seek Subscribing to topics {} with properties",topicsArray,properties);
		Future<List<ConsumerRecord<?, ?>>> future = consumerPool.submit(runner);
		return future.get();
	}

	
	@Processor(name = "Producer", friendlyName = "Producer")
	public List<RecordMetadata> producer(@Optional String topic, String key, String message, @Optional String propertyFileOverrides) throws ExecutionException, InterruptedException, EndpointException {
		Properties properties = copyProperties(getProducerProperties());
		if (null == topic){
			topic = properties.getProperty(KafkaConnectorConstants.PRODUCER_TOPIC);
		}
		if (null == topic){
			throw new EndpointException(MessageFactory.createStaticMessage("Topic cannot be null. Either pass in producer XMl or put 'producer.topic' in configuration file'"));
		}
		ProducerRecord producerRecord = new ProducerRecord(topic, key, message);
		logger.debug("Using producer record {} with properties {}",producerRecord,properties);
		List<ProducerRecord> producerRecords = Collections.singletonList(producerRecord);
		KafkaProducerRunner runner = new KafkaProducerRunner(properties, producerRecords);
		Future<List<RecordMetadata>> results = producerPool.submit(runner);
		return results.get();
	}

}
