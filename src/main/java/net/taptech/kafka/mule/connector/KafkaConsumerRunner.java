package net.taptech.kafka.mule.connector;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.mule.api.callback.SourceCallback;
import org.mule.api.endpoint.EndpointException;
import org.mule.config.i18n.MessageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaConsumerRunner implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(KafkaConsumerRunner.class);

    private static final int DEFAULT_CONSUMER_POLL_TIMEOUT = 1000;


    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicInteger count = new AtomicInteger(0);
    private KafkaConsumer<String, String> consumer;
    private Properties consumerProperties;
    private SourceCallback sourceCallback;

    public KafkaConsumerRunner(Properties consumerProperties, SourceCallback sourceCallback) {
        this.consumerProperties = consumerProperties;
        this.sourceCallback = sourceCallback;
        this.consumer = new KafkaConsumer<>(consumerProperties);
        String [] topicsArray = consumerProperties.getProperty(KafkaConnectorConstants.CONSUMER_TOPICS).split(",");
        logger.trace("KafkaConsumerRunner # {} Subscribing to topics {} ",count.incrementAndGet(),topicsArray);
        consumer.subscribe(Arrays.asList(topicsArray));
    }

    public void run() {
        try {

            while (!closed.get()) {
                Integer pollTimeOut = Integer.valueOf(consumerProperties.getProperty(KafkaConnectorConstants.CONSUMER_POLL_TIMEOUT));
                ConsumerRecords<?, ?> records = consumer.poll(pollTimeOut);
                if (logger.isDebugEnabled()){

                    logger.debug("Number of topic {} consumer records returned {} from partitions {}",new Object[]{consumerProperties.getProperty(KafkaConnectorConstants.CONSUMER_TOPICS),records.count(),records.partitions()});
                    for (ConsumerRecord<?, ?> record : records) {
                        System.out.printf("partition = %s,offset = %d, key = %s, value = %s%n",record.partition(), record.offset(), record.key(), record.value());
                    }

                }
                for (TopicPartition topicPartition:records.partitions()){
                    try {
                        sourceCallback.process(records.records(topicPartition));
                    } catch (Exception e) {
                        logger.error("Could not callback to Mule!",e);
                        throw new RuntimeException("Could not callback to Mule!",e);
                    }
                }

            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!closed.get()) throw e;
        }  finally {
            consumer.close();
        }
    }

    // Shutdown hook which can be called from a separate thread
    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }
}
