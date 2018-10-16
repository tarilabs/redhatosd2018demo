package redhatosd2018demo;

import java.math.BigDecimal;
import java.util.Scanner;

import javax.json.Json;
import javax.json.JsonObject;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import static redhatosd2018demo.Configs.producerConfig;
import static redhatosd2018demo.Configs.serverConfig;

public class Micro1 {

    private KafkaServerEmbedded server;
    private KafkaProducer producer;

    public void main() throws Exception {
        before();
        Scanner scanIn = new Scanner(System.in);
        producer = new KafkaProducer(producerConfig());
        while (true) {
            System.out.print("Age? ");
            String inputAge = scanIn.nextLine();

            System.out.print("Employed? ");
            String inputEmployed = scanIn.nextLine();

            System.out.print("Account balance? ");
            String inputBalance = scanIn.nextLine();

            // ---
            JsonObject context = Json.createObjectBuilder()
                                     .add("Account holder", Json.createObjectBuilder()
                                                                .add("age", new BigDecimal(inputAge))
                                                                .add("employed", Boolean.valueOf(inputEmployed)).build())
                                     .add("Account balance", new BigDecimal(inputBalance))
                                     .build();
            String contextAsJSON = context.toString();
            System.out.println("WRITING: " + contextAsJSON);
            // ---
            producer.send(new ProducerRecord(Configs.TOPIC_1, contextAsJSON)).get();
        }
    }

    public static void main(String[] args) throws Exception {
        new Micro1().main();
    }

    public void before() throws Exception {
        System.out.println("-- BEFORE -- ");
        server = new KafkaServerEmbedded();
        server.start(serverConfig());
    }

    public void after() throws Exception {
        System.out.println("-- AFTER -- ");
        if (producer != null) {
            producer.close();
        }
        server.stop();
    }
}
