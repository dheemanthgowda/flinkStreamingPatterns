package com.learning.flinkstreaming.chapter4;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/****************************************************************************
 * This Generator generates a a series of Players and scores
 * into Kafka at random intervals
 * This can be used to test Leaderboard pipelines
 ****************************************************************************/

public class KafkaGamingDataGenerator implements Runnable {


    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_BLUE = "\u001B[34m";

    public static final String topic = "streaming.leaderboards.input";

    public static void main(String[] args) {
        KafkaGamingDataGenerator kodg = new KafkaGamingDataGenerator();
        kodg.run();
    }

    public void run() {

        try {

            System.out.println("Starting Kafka Gaming Generator..");
            //Wait for the main flow to be setup.
            Thread.sleep(5000);

            //Setup Kafka Client
            Properties kafkaProps = new Properties();
            kafkaProps.put("bootstrap.servers","localhost:9092");

            kafkaProps.put("key.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer");
            kafkaProps.put("value.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer");

            Producer<String,String> myProducer
                    = new KafkaProducer<String, String>(kafkaProps);

            //Define list of players
            List<String> players = new ArrayList<String>();
            players.add("Bob");
            players.add("Mike");
            players.add("Kathy");
            players.add("Sam");

            //Define a random number generator
            Random random = new Random();

            //Capture current timestamp
            String currentTime = String.valueOf(System.currentTimeMillis());

            //Generate 100 sample order records
            for(int i=0; i < 100; i++) {

                 //Generate a random player & score
                String player =players.get(random.nextInt(players.size()));
                int score = random.nextInt(10) + 1;

                //Use player as key. Each player will go to the same partition
                //Hence the updates for a given player are sequencial
                String recKey = String.valueOf(player);
                String value = String.valueOf(score);

                ProducerRecord<String, String> record =
                        new ProducerRecord<String, String>(
                                topic,
                                recKey,
                                value );

                RecordMetadata rmd = myProducer.send(record).get();

                System.out.println(ANSI_PURPLE +
                            "Kafka Gaming Stream Generator : Sending Event : "
                            + recKey + " = " + value  + ANSI_RESET);

                //Sleep for a random time ( 1 - 3 secs) before the next record.
                Thread.sleep(random.nextInt(2000) + 1000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
