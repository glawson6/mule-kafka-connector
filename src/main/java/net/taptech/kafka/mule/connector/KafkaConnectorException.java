package net.taptech.kafka.mule.connector;

public class KafkaConnectorException extends RuntimeException{

    public KafkaConnectorException(String message) {
        super(message);
    }

    public KafkaConnectorException(String message, Throwable cause) {
        super(message, cause);
    }

}
