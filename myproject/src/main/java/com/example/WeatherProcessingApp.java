package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Properties;

public class WeatherProcessingApp {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-processing-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        
        KStream<String, String> weatherStream = builder.stream("weather-data");
        
        KGroupedStream<String, String> groupedByStation = weatherStream
            .groupBy((key, value) -> {
                String[] parts = value.split(",");
                return parts.length == 3 ? parts[0] : "unknown";
            });
        
        KTable<String, String> averageTable = groupedByStation
            .aggregate(
                () -> new StationStats(),
                (station, value, aggregate) -> {
                    String[] parts = value.split(",");
                    if (parts.length == 3) {
                        try {
                            double temp = Double.parseDouble(parts[1]);
                            double humidity = Double.parseDouble(parts[2]);
                            aggregate.addReading(temp, humidity);
                        } catch (NumberFormatException e) {
                            // Ignore invalid data
                        }
                    }
                    return aggregate;
                },
                Materialized.<String, StationStats, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("station-stats-store")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.serdeFrom(new StationStatsSerializer(), new StationStatsDeserializer()))
            )
            .mapValues(stats -> String.format("%s,%.1f,%.1f", 
                stats.getStation(), stats.getAverageTemp(), stats.getAverageHumidity()));
        
        averageTable.toStream().to("station-averages");
        
        averageTable.toStream().foreach((station, avgData) -> {
            System.out.println("Moyennes station " + station + ": " + avgData);
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
    
    static class StationStats {
        private double totalTemp = 0;
        private double totalHumidity = 0;
        private int count = 0;
        private String station;
        
        public void addReading(double temp, double humidity) {
            this.totalTemp += temp;
            this.totalHumidity += humidity;
            this.count++;
        }
        
        public double getAverageTemp() {
            return count > 0 ? totalTemp / count : 0;
        }
        
        public double getAverageHumidity() {
            return count > 0 ? totalHumidity / count : 0;
        }
        
        public String getStation() {
            return station;
        }
    }
}