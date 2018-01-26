package net.taptech.kafka.mule.connector;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaProducerRunner implements Callable<List<RecordMetadata>> {

    private static Logger logger = LoggerFactory.getLogger(KafkaProducerRunner.class);

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private KafkaProducer producer;
    private Properties producerProperties;
    private List<ProducerRecord> producerRecords;

    public KafkaProducerRunner(Properties producerProperties,List<ProducerRecord> producerRecords) {
        this.producerProperties = producerProperties;
        this.producerRecords = producerRecords;

        logger.trace("Creating KafkaProducer with properties {} and producer records {}",producerProperties, producerRecords);
        this.producer = new KafkaProducer(producerProperties);
    }

    public List<ProducerRecord> getProducerRecords() {
        return producerRecords;
    }

    public KafkaProducer getProducer() {
        return producer;
    }

    public Properties getProducerProperties() {
        return producerProperties;
    }

    public void shutdown() {
        getProducer().close();
    }


    @Override
    public List<RecordMetadata> call() throws Exception {
        List<RecordMetadata> results = new ArrayList<>();
        Properties properties = getProducerProperties();
        for (ProducerRecord producerRecord:getProducerRecords()){
            Future<RecordMetadata> result =  producer.send(producerRecord);
            results.add(result.get());
        }
        return results;
    }
}
