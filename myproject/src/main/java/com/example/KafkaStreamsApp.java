package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.Set;

public class KafkaStreamsApp {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "text-processing-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        
        KStream<String, String> textInputStream = builder.stream("text-input");
        
        textInputStream.foreach((key, value) -> {
            System.out.println("Message reçu - Key: " + key + ", Value: " + value);
        });
        
        Set<String> forbiddenWords = Set.of("HACK", "SPAM", "XXX");
        
        // Stream pour les messages valides
        KStream<String, String> validStream = textInputStream
            .filter((key, value) -> {
                if (value == null || value.trim().isEmpty()) {
                    System.out.println("Message rejeté - vide ou espaces uniquement: '" + value + "'");
                    return false;
                }
                return true;
            })
            .filter((key, value) -> {
                String upperValue = value.toUpperCase();
                for (String word : forbiddenWords) {
                    if (upperValue.contains(word)) {
                        System.out.println("Message rejeté - contient mot interdit '" + word + "': '" + value + "'");
                        return false;
                    }
                }
                return true;
            })
            .filter((key, value) -> {
                if (value.length() > 100) {
                    System.out.println("Message rejeté - dépasse 100 caractères: '" + value + "'");
                    return false;
                }
                return true;
            })
            .mapValues(value -> {
                String cleaned = value.trim();
                cleaned = cleaned.replaceAll("\\s+", " ");
                cleaned = cleaned.toUpperCase();
                System.out.println("Message traité - Original: '" + value + "' -> Nettoyé: '" + cleaned + "'");
                return cleaned;
            });

        // Stream pour les messages invalides
        KStream<String, String> invalidStream = textInputStream
            .filter((key, value) -> {
                if (value == null || value.trim().isEmpty()) {
                    return true;
                }
                String upperValue = value.toUpperCase();
                for (String word : forbiddenWords) {
                    if (upperValue.contains(word)) {
                        return true;
                    }
                }
                return value.length() > 100;
            });

        // Routage
        validStream.to("text-clean");
        invalidStream.to("text-dead-letter");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}