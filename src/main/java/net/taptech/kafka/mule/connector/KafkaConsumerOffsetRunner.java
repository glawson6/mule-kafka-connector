package net.taptech.kafka.mule.connector;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.mule.api.callback.SourceCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaConsumerOffsetRunner implements Callable<List<ConsumerRecord<?, ?>>> {
    private static Logger logger = LoggerFactory.getLogger(KafkaConsumerOffsetRunner.class);

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private KafkaConsumer<String, String> consumer;
    private Properties consumerProperties;

    public KafkaConsumerOffsetRunner(Properties consumerProperties) {
        this.consumerProperties = (Properties) consumerProperties.clone();
        this.consumer = new KafkaConsumer<>(consumerProperties);
        String[] topicsArray = consumerProperties.getProperty(KafkaConnectorConstants.CONSUMER_TOPICS).split(",");
        logger.debug("KafkaConsumerOffsetRunner Subscribing to topics {} with properties {}", topicsArray,consumerProperties);
    }

    @Override
    public List<ConsumerRecord<?, ?>> call()  {
        List<ConsumerRecord<?, ?>> consumerRecords = new ArrayList<>();
        try {

            Integer pollTimeOut = Integer.valueOf(consumerProperties.getProperty(KafkaConnectorConstants.CONSUMER_POLL_TIMEOUT));
            String consumerTopic = consumerProperties.getProperty(KafkaConnectorConstants.CONSUMER_TOPIC);
            Integer partition = Integer.valueOf(consumerProperties.get(KafkaConnectorConstants.CONSUMER_PARTITION).toString());
            Integer offset = Integer.valueOf(consumerProperties.get(KafkaConnectorConstants.CONSUMER_OFFSET).toString());
            TopicPartition topicPartitionAssign = new TopicPartition(consumerTopic, partition);
            consumer.assign(Collections.singletonList(topicPartitionAssign));
            consumer.seek(topicPartitionAssign, offset);
            ConsumerRecords<?, ?> records = consumer.poll(pollTimeOut);
            if (logger.isDebugEnabled()) {
                logger.debug("Number of topic {} consumer records returned {} from partitions {}", new Object[]{consumerProperties.getProperty(KafkaConnectorConstants.CONSUMER_TOPICS), records.count(), records.partitions()});
                for (ConsumerRecord<?, ?> record : records) {
                    logger.debug("partition = {},offset = {}, key = {}, value = {}", record.partition(), record.offset(), record.key(), record.value());
                }
            }

            for (TopicPartition topicPartition : records.partitions()) {
                try {
                    consumerRecords.addAll(records.records(topicPartition));
                } catch (Exception e) {
                    logger.error("Could not consume records!", e);
                    throw new KafkaConnectorException("Could not consume records!", e);
                }
            }

        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!closed.get()) throw new KafkaConnectorException("Exception waking up! ",e);
        }  catch (Exception e) {
            if (logger.isDebugEnabled()){
                e.printStackTrace();
            }
            e.printStackTrace();
            throw new KafkaConnectorException("Error in call! ",e);
        }  finally {
            consumer.close();
        }
        return consumerRecords;
    }
}
