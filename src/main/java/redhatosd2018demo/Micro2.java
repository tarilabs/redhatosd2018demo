package redhatosd2018demo;

import java.io.StringReader;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.kie.dmn.api.core.DMNContext;
import org.kie.dmn.api.core.DMNDecisionResult;
import org.kie.dmn.api.core.DMNResult;
import org.kie.server.api.marshalling.MarshallingFormat;
import org.kie.server.api.model.ServiceResponse;
import org.kie.server.client.DMNServicesClient;
import org.kie.server.client.KieServicesClient;
import org.kie.server.client.KieServicesConfiguration;
import org.kie.server.client.KieServicesFactory;

import static redhatosd2018demo.Configs.consumerConfig;
import static redhatosd2018demo.Configs.producerConfig;
import static redhatosd2018demo.Utils.entry;
import static redhatosd2018demo.Utils.mapOf;

public class Micro2 {

    private ConsumerConnector consumerConnector;
    private KafkaProducer producer;

    private static final String URL = "http://rhdm7-install-kieserver-rhdm7-install-developer.192.168.42.81.nip.io/services/rest/server";
    private static final String USER = "kieserver";
    private static final String PASSWORD = "kieserver1!";
    private static final String CONTAINER_ID = "demo20181016_1.0.0";

    private static final MarshallingFormat FORMAT = MarshallingFormat.JSON;

    private KieServicesConfiguration conf;
    private KieServicesClient kieServicesClient;

    public void initialize() {
        conf = KieServicesFactory.newRestConfiguration(URL, USER, PASSWORD);
        conf.setMarshallingFormat(FORMAT);
        kieServicesClient = KieServicesFactory.newKieServicesClient(conf);
    }

    public void main() throws Exception {
        before();
        ConsumerIterator<String, String> it = buildConsumer(Configs.TOPIC_1);
        producer = new KafkaProducer(producerConfig());

        initialize();
        DMNServicesClient dmnClient = kieServicesClient.getServicesClient(DMNServicesClient.class);
        while (true) {
            // --- READ FROM KAFKA:
            if (it.hasNext()) {
                MessageAndMetadata<String, String> messageAndMetadata = it.next();
                String value = messageAndMetadata.message();
                // ---

                DMNContext dmnContext = dmnClient.newContext();
                try (StringReader sr = new StringReader(value); JsonReader rdr = Json.createReader(sr)) {
                    JsonObject obj = rdr.readObject();
                    System.out.println("READ: " + obj);
                    JsonObject accountHolder = obj.getJsonObject("Account holder");
                    BigDecimal accountBalance = obj.getJsonNumber("Account balance").bigDecimalValue();

                    dmnContext.set("Account holder", mapOf(entry("age", accountHolder.getJsonNumber("age").bigDecimalValue()),
                                                           entry("employed", accountHolder.getBoolean("employed"))));
                    dmnContext.set("Account balance", accountBalance);

                    ServiceResponse<DMNResult> serverResp = dmnClient.evaluateAll(CONTAINER_ID, dmnContext);

                    DMNResult dmnResult = serverResp.getResult();

                    for (DMNDecisionResult dr : dmnResult.getDecisionResults()) {
                        System.out.println("--------------------------------------------");
                        System.out.println("Decision name:   " + dr.getDecisionName());
                        System.out.println("Decision status: " + dr.getEvaluationStatus());
                        System.out.println("Decision result: " + dr.getResult());
                    }
                    // ---
                    JsonObject context = Json.createObjectBuilder(obj)
                                             .add("Processed", "processed.")
                                             .add("Account Profile", dmnResult.getDecisionResultByName("Account Profile").getResult().toString())
                                             .add("Exemptions", dmnResult.getDecisionResultByName("Exemptions").getResult().toString())
                                             .add("Monthly fee", dmnResult.getDecisionResultByName("Monthly fee").getResult().toString())
                                             .build();
                    String contextAsJSON = context.toString();
                    System.out.println("WRITING: " + contextAsJSON);
                    // ---
                    producer.send(new ProducerRecord(Configs.TOPIC_2, contextAsJSON)).get();
                }
            } else {
                System.out.println("waiting...");
                Thread.sleep(5_000);
            }

        }
    }

    public static void main(String[] args) throws Exception {
        new Micro2().main();
    }

    public void before() throws Exception {
        System.out.println("-- BEFORE -- ");
    }

    public void after() throws Exception {
        System.out.println("-- AFTER -- ");
        if (producer != null) {
            producer.close();
        }
        if (consumerConnector != null) {
            consumerConnector.shutdown();
        }
    }

    private ConsumerIterator<String, String> buildConsumer(String topic) {
        Properties props = consumerConfig();
        Map<String, Integer> topicCountMap = new HashMap();
        topicCountMap.put(topic, 1);
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
        Map<String, List<KafkaStream<String, String>>> consumers = consumerConnector.createMessageStreams(topicCountMap, new StringDecoder(null), new StringDecoder(null));
        KafkaStream<String, String> stream = consumers.get(topic).get(0);
        return stream.iterator();
    }
}
