package com.learning.flinkstreaming.chapter6;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/****************************************************************************
 * This Generator generates a webpage view events
 * into Kafka at random intervals
 * This can be used to test Streaming pipelines
 ****************************************************************************/

public class KafkaViewsDataGenerator implements Runnable {


    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_BLUE = "\u001B[34m";

    public static final String kafkaTopic = "streaming.views.input";

    public static void main(String[] args) {
        KafkaViewsDataGenerator kodg = new KafkaViewsDataGenerator();
        kodg.run();
    }

    public void run() {

        try {

            System.out.println("Starting Kafka Views Generator..");
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

            //Define list of Products
            List<String> users = new ArrayList<String>();
            users.add("Bob");
            users.add("Mike");
            users.add("Kathy");
            users.add("Sam");

            List<String> topics = new ArrayList<String>();
            topics.add("AI");
            topics.add("BigData");
            topics.add("CI_CD");
            topics.add("Cloud");

            //Define a random number generator
            Random random = new Random();

            int recKey = (int)Math.floor(System.currentTimeMillis()/1000);

            //Generate 100 sample order records
            for(int i=0; i < 100; i++) {

                recKey++;

                //Capture current timestamp
                Timestamp currTimeStamp = new Timestamp(System.currentTimeMillis());
                String user = users.get(random.nextInt(users.size()));
                String topic = topics.get(random.nextInt(topics.size()));
                int minutes = random.nextInt(10) + 1;

                //Form a CSV
                String value= "\"" + currTimeStamp.toString() + "\","
                        +  "\"" + user + "\","
                        +  "\"" + topic + "\","
                        +  minutes  ;


                ProducerRecord<String, String> record =
                        new ProducerRecord<String, String>(
                                kafkaTopic,
                                String.valueOf(recKey),
                                value );

                RecordMetadata rmd = myProducer.send(record).get();

                System.out.println(ANSI_PURPLE +
                        "Kafka Views Stream Generator : Sending Event : "
                        + String.join(",", value)  + ANSI_RESET);

                //Sleep for a random time ( 1 - 3 secs) before the next record.
                Thread.sleep(random.nextInt(2000) + 1000);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
