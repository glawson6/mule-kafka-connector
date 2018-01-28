package net.taptech.kafka.mule.connector

import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.mule.api.MuleEvent
import org.mule.api.MuleException
import org.mule.api.callback.SourceCallback
import org.springframework.core.io.DefaultResourceLoader
import org.springframework.core.io.ResourceLoader

class TestUtils {
    public static Map<String, Object> senderProps(String brokers) {
        Map<String, Object> props = new HashMap();
        props.put("bootstrap.servers", brokers);
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return props;
    }

    public static ResourceLoader resourceLoader = new DefaultResourceLoader();

    public static loadProperties(Properties properties,String  resourceName){
        properties.load(resourceLoader.getResource(resourceName).inputStream)
        properties
    }

    def static final consumerProperties =  loadProperties(new Properties(),"kafka-consumer.properties")
    def static final producerProperties = loadProperties(new Properties(),"kafka-producer.properties")

    public static class MuleSourceCallback implements SourceCallback{

        Object returnObject;

        MuleSourceCallback(Object returnObject) {
            this.returnObject = returnObject
        }

        @Override
        Object process() throws Exception {
            return returnObject
        }

        @Override
        Object process(Object o) throws Exception {
            return returnObject
        }

        @Override
        Object process(Object o, Map<String, Object> map) throws Exception {
            return returnObject
        }

        @Override
        MuleEvent processEvent(MuleEvent muleEvent) throws MuleException {
            return returnObject
        }
    }
}
