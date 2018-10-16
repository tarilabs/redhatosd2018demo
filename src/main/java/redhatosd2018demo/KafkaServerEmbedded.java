package redhatosd2018demo;

import java.io.IOException;
import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.curator.test.TestingServer;

public class KafkaServerEmbedded {

    private TestingServer zk;
    private KafkaServerStartable kafka;

    public void start(Properties p) throws Exception {
        Integer port = getZkPort(p);
        zk = new TestingServer(port);
        zk.start();

        KafkaConfig config = new KafkaConfig(p);
        kafka = new KafkaServerStartable(config);
        kafka.startup();
    }

    public void stop() throws IOException {
        kafka.shutdown();
        zk.stop();
        zk.close();
    }

    private int getZkPort(Properties p) {
        String url = (String) p.get("zookeeper.connect");
        String port = url.split(":")[1];
        return Integer.valueOf(port);
    }

}
