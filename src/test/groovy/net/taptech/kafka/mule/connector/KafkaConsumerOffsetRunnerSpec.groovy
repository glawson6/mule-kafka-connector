package net.taptech.kafka.mule.connector

import org.apache.commons.io.IOUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.junit.ClassRule
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.core.io.DefaultResourceLoader
import org.springframework.core.io.ResourceLoader
import org.springframework.kafka.test.rule.KafkaEmbedded
import org.springframework.kafka.test.utils.KafkaTestUtils
import spock.lang.Specification

class KafkaConsumerOffsetRunnerSpec extends Specification {

    private static Logger logger = LoggerFactory.getLogger(KafkaConsumerOffsetRunnerSpec.class);
    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(2, true, 2, "messages","topic-1");

    def "Call"() {

        given:"A KafkaConsumerOffsetRunner"
        Map<String, Object> senderProps = TestUtils.senderProps("127.0.0.1:9092");
        KafkaProducer<String, String> producer = new KafkaProducer<>(senderProps);
        RecordMetadata[] recordMetadata = new RecordMetadata[4]
        recordMetadata[0] = producer.send(new ProducerRecord<>("messages", 0, "0", "message0")).get();
        recordMetadata[1] = producer.send(new ProducerRecord<>("messages", 0, "1", "message1")).get();
        recordMetadata[2] = producer.send(new ProducerRecord<>("messages", 0, "2", "message2")).get();
        recordMetadata[3] = producer.send(new ProducerRecord<>("messages", 0, "3", "message3")).get();

        Properties properties = TestUtils.consumerProperties.clone()
        properties.put(KafkaConnectorConstants.CONSUMER_PARTITION, new Integer(recordMetadata[1].partition()));
        properties.put(KafkaConnectorConstants.CONSUMER_OFFSET, new Integer(recordMetadata[1].offset().intValue()));
        properties.put(KafkaConnectorConstants.CONSUMER_TOPIC, recordMetadata[1].topic());

        def offsetRunner = new KafkaConsumerOffsetRunner(properties)

        when:"call() is made"
        def results = offsetRunner.call()

        then:
        results != null
        results instanceof List
        results.size() == 3
        ConsumerRecord consumerRecord = results.get(2)
        consumerRecord.key() == "3"
        consumerRecord.value() == "message3"
    }


}
