package com.learning.flinkstreaming.leaderboardPattern;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/****************************************************************************
 * This example is an example for Streaming leaderboards in Flink.
 * It reads a real time player scores stream from kafka
 * and updates a sortedset in Redis
 ****************************************************************************/

public class StreamingLeaderboards {
    public static void main(String[] args){

        try {

            System.out.println("******** Initiating Streaming Leaderboards *************");

            // Set up the streaming execution environment
            final StreamExecutionEnvironment streamEnv
                    = StreamExecutionEnvironment.getExecutionEnvironment();

            //This many number of connections will be open for Redis
            //for updates
            System.out.println("Parallelism = " + streamEnv.getParallelism());

            //Initiate  RedisManager for printing leaderboards
            RedisManager redisManager = new RedisManager();
            redisManager.setUp();
            Thread redisThread = new Thread(redisManager);
            redisThread.start();

            //Initiate the Kafka Gaming Data Generator
            KafkaGamingDataGenerator scoresGenerator = new KafkaGamingDataGenerator();
            Thread genThread = new Thread(scoresGenerator);
            genThread.start();

            //Set connection properties to Kafka Cluster
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", "localhost:9092");
            properties.setProperty("group.id", "flink.streaming.realtime");

            //Setup a Kafka Consumer on Flnk
            FlinkKafkaConsumer<PlayerScore> kafkaConsumer =
                    new FlinkKafkaConsumer<PlayerScore>
                            ("streaming.leaderboards.input", //topic
                                    new KafkaDeserializationSchema<PlayerScore>() {
                                        @Override
                                        public TypeInformation<PlayerScore> getProducedType() {
                                            return TypeInformation.of(PlayerScore.class);
                                        }
                                        @Override
                                        public boolean isEndOfStream(PlayerScore ps) {
                                            return false;
                                        }

                                        @Override
                                        public PlayerScore deserialize(
                                                ConsumerRecord<byte[], byte[]> consumerRecord)
                                                throws Exception {

                                            PlayerScore ps = new PlayerScore();
                                            ps.setPlayer(new String(consumerRecord.key()));
                                            ps.setScore(Long.valueOf(new String(consumerRecord.value())));

                                            System.out.println("Received Score: " + ps.toString());

                                            return ps;
                                        }
                                    },
                                    properties); //connection properties

            //Setup to receive only new messages
            kafkaConsumer.setStartFromLatest() ;

            //Create the data stream
            DataStream<PlayerScore> scoresInput = streamEnv
                    .addSource(kafkaConsumer);

            //Use a RichSinkFunction to write to Redis
            scoresInput
                    .addSink(new RichSinkFunction<PlayerScore>() {

                        RedisManager redisUpdater;
                        @Override
                        public void open(Configuration parameters)
                                    throws Exception {
                            super.open(parameters);
                            redisUpdater = new RedisManager();
                            redisUpdater.setUp();
                        }

                        @Override
                        public void close() throws Exception {
                            super.close();
                        }

                        @Override
                        public void invoke(PlayerScore playerScore,
                                           Context context)
                                        throws Exception {
                            //Update score in Redis
                            redisUpdater.update_score(
                                    playerScore.getPlayer(),
                                    Double.valueOf(playerScore.getScore())
                            );
                        }
                    });

            // execute the streaming pipeline
            streamEnv.execute("Flink Leaderboards");

            final CountDownLatch latch = new CountDownLatch(1);
            latch.await();
        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }
}
