package net.taptech.kafka.mule.connector.config;

import org.mule.api.MuleContext;
import org.mule.api.annotations.Configurable;
import org.mule.api.annotations.components.Configuration;
import org.mule.api.annotations.param.Default;
import org.mule.api.context.MuleContextAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Configuration(friendlyName = "Configuration")
public class ConnectorConfig implements MuleContextAware {

	private static Logger logger = LoggerFactory.getLogger(ConnectorConfig.class);

	public ConnectorConfig() {
		logger.info("Creating ConnectorConfig");
	}

	/**
	 * Greeting message
	 */
	@Configurable
	@Default("kafka-consumer.properties")
	private String consumerPropertiesFile;

	/**
	 * Reply message
	 */
	@Configurable
	@Default("kafka-producer.properties")
	private String producerPropertiesFile;

	public String getConsumerPropertiesFile() {
		return consumerPropertiesFile;
	}

	public void setConsumerPropertiesFile(String consumerPropertiesFile) {
		this.consumerPropertiesFile = consumerPropertiesFile;
	}

	public String getProducerPropertiesFile() {
		return producerPropertiesFile;
	}

	public void setProducerPropertiesFile(String producerPropertiesFile) {
		this.producerPropertiesFile = producerPropertiesFile;
	}

	private MuleContext muleContext;

	@Override
	public void setMuleContext(MuleContext context) {
		logger.info("Setting MuleContext!!!!!!!!!!!");
		this.muleContext = context;
	}

    public MuleContext getMuleContext() {
        return muleContext;
    }
}