package redhatosd2018demo;

import java.util.Properties;

import org.kie.server.api.marshalling.MarshallingFormat;

public class Configs {

    public static final String URL = "http://rhdm7-qlb-loan-kieserver-rhdm7-qlb-loan-developer.192.168.42.85.nip.io/services/rest/server";
    public static final String USER = "kieserver";
    public static final String PASSWORD = "kieserver1!";
    public static final String CONTAINER_ID = "demo20181016_1.0.0";
    public static final MarshallingFormat FORMAT = MarshallingFormat.JSON;

    public static final String TOPIC_1 = "demo20181016-topic1";
    public static final String TOPIC_2 = "demo20181016-topic2";

    public static Properties consumerConfig() {
        Properties p = new Properties();
        p.put("zookeeper.connect", serverConfig().get("zookeeper.connect"));
        p.put("group.id", "group1");
        p.put("auto.offset.reset", "smallest");
        return p;
    }

    public static Properties producerConfig() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "localhost:9092");
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("request.required.acks", "1");
        return p;
    }

    public static Properties serverConfig() {
        Properties p = new Properties();
        p.put("zookeeper.connect", "localhost:2181");
        p.put("broker.id", "1");
        return p;
    }

    private Configs() {
        // private by intent.
    }
}
