package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class KafkaStreamsApp {

    public static void main(String[] args) {
        // Configuration de Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "text-processing-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Construction du stream
        StreamsBuilder builder = new StreamsBuilder();
        
        // Lire les messages du topic text-input
        KStream<String, String> textInputStream = builder.stream("text-input");
        
        // Afficher les messages lus (pour vérification)
        textInputStream.foreach((key, value) -> {
            System.out.println("Message reçu - Key: " + key + ", Value: " + value);
        });

        // Démarrer l'application Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Ajouter un hook pour arrêter proprement l'application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}