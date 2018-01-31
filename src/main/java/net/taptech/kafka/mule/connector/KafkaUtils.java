package net.taptech.kafka.mule.connector;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.mule.util.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class KafkaUtils {
    private static Logger logger = LoggerFactory.getLogger(KafkaUtils.class);

    public static Collection<Header> extractHeaders(Map<String, Object> headerMap) {
        logger.debug("We got a map {}", headerMap);
        List<Header> headers = new ArrayList<>();
        if (null != headerMap && headerMap.size() > 0) {
            for (Map.Entry<String, Object> entry : headerMap.entrySet()) {
                headers.add(new RecordHeader(entry.getKey(), SerializationUtils.serialize((Serializable) entry.getValue())));
            }
        }

        return headers;
    }
}
