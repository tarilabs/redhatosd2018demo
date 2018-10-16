package redhatosd2018demo;

import java.io.StringReader;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

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
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.dmn.api.core.DMNContext;
import org.kie.dmn.api.core.DMNModel;
import org.kie.dmn.api.core.DMNResult;
import org.kie.dmn.api.core.DMNRuntime;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static redhatosd2018demo.Configs.consumerConfig;
import static redhatosd2018demo.Configs.producerConfig;
import static redhatosd2018demo.Configs.serverConfig;
import static redhatosd2018demo.Utils.entry;
import static redhatosd2018demo.Utils.mapOf;

public class KafkaDMNTest {

    @Test
    public void shouldWriteThenRead() throws Exception {

        //Create a consumer
        ConsumerIterator<String, String> it = buildConsumer(topic);

        //Create a producer
        producer = new KafkaProducer(producerConfig());

        //send a message
        producer.send(new ProducerRecord(topic, "message")).get();

        //read it back
        MessageAndMetadata<String, String> messageAndMetadata = it.next();
        String value = messageAndMetadata.message();
        assertThat(value, is("message"));
    }

    @Test
    public void testBasic() throws InterruptedException, ExecutionException {
        KieServices kieServices = KieServices.Factory.get();

        KieContainer kieContainer = kieServices.getKieClasspathContainer();

        DMNRuntime dmnRuntime = kieContainer.newKieSession().getKieRuntime(DMNRuntime.class);

        DMNModel dmnModel = dmnRuntime.getModel("http://www.trisotech.com/dmn/definitions/_73732c1d-f5ff-4219-a705-f551a5161f88", "Bank monthly fees");

        DMNContext dmnContext = dmnRuntime.newContext();

        // ---
        JsonObject context = Json.createObjectBuilder()
                                 .add("Account holder", Json.createObjectBuilder()
                                                            .add("age", BigDecimal.valueOf(36))
                                                            .add("employed", Boolean.TRUE).build())
                                 .add("Account balance", BigDecimal.valueOf(10_000))
                                 .build();
        String contextAsJSON = context.toString();
        System.out.println(contextAsJSON);
        // ---
        producer = new KafkaProducer(producerConfig());
        producer.send(new ProducerRecord(topic, contextAsJSON)).get();
        ConsumerIterator<String, String> it = buildConsumer(topic);
        MessageAndMetadata<String, String> messageAndMetadata = it.next();
        String value = messageAndMetadata.message();
        // ---

        try (StringReader sr = new StringReader(value); JsonReader rdr = Json.createReader(sr)) {
            JsonObject obj = rdr.readObject();
            JsonObject accountHolder = obj.getJsonObject("Account holder");
            BigDecimal accountBalance = obj.getJsonNumber("Account balance").bigDecimalValue();

            dmnContext.set("Account holder", mapOf(entry("age", accountHolder.getJsonNumber("age").bigDecimalValue()),
                                                   entry("employed", accountHolder.getBoolean("employed"))));
            dmnContext.set("Account balance", accountBalance);
        }

        DMNResult dmnResult = dmnRuntime.evaluateAll(dmnModel, dmnContext);
        DMNTest.assertResult(dmnResult);
    }



    private KafkaServerEmbedded server;
    private String topic;
    private Producer producer;
    private ConsumerConnector consumerConnector;

    @Before
    public void before() throws Exception {
        System.out.println("-- BEFORE -- ");
        topic = "topic1-" + System.currentTimeMillis();
        server = new KafkaServerEmbedded();
        server.start(serverConfig());
    }

    @After
    public void after() throws Exception {
        System.out.println("-- AFTER -- ");
        if (producer != null) {
            producer.close();
        }
        if (consumerConnector != null) {
            consumerConnector.shutdown();
        }
        server.stop();
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
