package com.datastax.kellett;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class EventGenerator
{
    public static void main (String[] args) throws InterruptedException, FileNotFoundException
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all"); // See https://kafka.apache.org/documentation/
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // The input file contains 100K rows of data that represents modifications to the
        // RightVest investment instrument ratings
        Producer<String, String> producer = new KafkaProducer<>(props);

        Scanner sc = new Scanner(new FileReader(
                "data/spark_streaming_workshop.rating_modifiers.csv"));

        while (sc.hasNextLine())
        {
            String lineInput = sc.nextLine();
            String[] parsedInput = lineInput.split(",");

            producer.send(new ProducerRecord<>("rating_modifiers", parsedInput[1], lineInput));
            Thread.sleep(500);
            System.out.println("emitted " + parsedInput[1] + " " + lineInput);
        }
        sc.close();
        producer.close();
    }
}
