package com.tigerconnect.pulsartools;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

@Slf4j
public class Main {

    static PulsarClient pclient;
    static Producer<String> producer;

    public static void main(String[] args) throws PulsarClientException {

        if (args.length != 3) {
            log.error("This tool requires 3 arguments.  \n[input-file] [pulsar-url] [topic]");
            System.exit(1);
        }
        String inputFile = args[0];
        String pulsarUrl = args[1];
        String topic = args[2];

        pclient = createPulsarClient(pulsarUrl);
        producer = pclient.newProducer(Schema.STRING).topic(topic).create();

        log.info("Loading messages (1 per line) from {}, sending to {} on {}", inputFile, topic, pulsarUrl);

        Path path = Paths.get(inputFile);

        try (Stream<String> lines = Files.lines(path)) {
            lines.forEachOrdered(Main::sendMessage);
        } catch (IOException e) {
            log.error("error", e);
        }

        producer.close();
        pclient.close();

        log.info("Done");

        System.exit(0);

    }

    public static void sendMessage(String payload) {
        sendMessage(payload, 3);
    }

    public static void sendMessage(String payload, int attempts) {
        try {
            producer.send(payload);
            log.info("Sent: {}", payload);

        } catch (Exception e) {
            log.error("Failed sending {}, trying again", payload);
            if (attempts > 0) {
                sendMessage(payload, attempts-1);
            }
        }
    }

    public static PulsarClient createPulsarClient(String url) {

        try {
            return PulsarClient.builder()
                    .serviceUrl(url)
                    .build();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }
}
