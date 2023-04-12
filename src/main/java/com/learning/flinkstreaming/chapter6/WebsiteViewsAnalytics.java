package com.learning.flinkstreaming.chapter6;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/****************************************************************************
 * This is an example for Website Views Analytics in Spark.
 * It reads a real time views from Kafka, computes 5 second user summaries
 * and keeps track of a leaderboard for topics with maximum views
 ****************************************************************************/
public class WebsiteViewsAnalytics {

    public static void main(String[] args){

        try {

            System.out.println("******** Initiating Website Views Analytics *************");

            // Set up the streaming execution environment
            final StreamExecutionEnvironment streamEnv
                    = StreamExecutionEnvironment.getExecutionEnvironment();

            //Initiate  RedisManager for printing leaderboards
//            RedisManager redisManager = new RedisManager();
//            redisManager.setUp();
//            Thread redisThread = new Thread(redisManager);
//            redisThread.start();

            //Set connection properties to Kafka Cluster
            Properties srcProps = new Properties();
            srcProps.setProperty("bootstrap.servers", "localhost:9092");
            srcProps.setProperty("group.id", "flink.streaming.realtime");

            //Initiate the Kafka Views Generator
//            KafkaViewsDataGenerator viewsGenerator = new KafkaViewsDataGenerator();
//            Thread genThread = new Thread(viewsGenerator);
//            genThread.start();

            //Setup a Kafka Consumer on Flnk
            FlinkKafkaConsumer<String> kafkaConsumer =
                    new FlinkKafkaConsumer<>
                            ("streaming.views.input", //topic
                                    new SimpleStringSchema(),
                                    srcProps); //connection properties

            //Setup to receive only new messages
            kafkaConsumer.setStartFromLatest() ;

            //Create the data stream
            DataStream<String> viewsRawInput =
                    streamEnv.addSource(kafkaConsumer);

            //Extract information from CSV String
            DataStream<Tuple4<String,String,String,Integer>> viewsData =
                    viewsRawInput
                            .map(new MapFunction<String, Tuple4<String, String, String, Integer>>() {
                                @Override
                                public Tuple4<String, String, String, Integer> map(String csvInput)
                                        throws Exception {

//                                    System.out.println("Received Data: " + csvInput);

                                    String[] viewsArray = csvInput
                                            .replace("\"","")
                                            .split(",");
                                    String timestamp = viewsArray[0];
                                    String user = viewsArray[1];
                                    String topic = viewsArray[2];
                                    Integer minutes = Integer.valueOf(viewsArray[3]);

                                    return new Tuple4<String,String,String,Integer>
                                            (timestamp,user,topic,minutes);
                                }
                            });

            //Compute Userwise total minutes by 5 seconds
            DataStream<Tuple3<String,String,Integer>> userWindowedSummary =
                    viewsData
                            //Extract User and count
                            .map(new MapFunction<Tuple4<String,String,String,Integer>,
                                         Tuple2<String, Integer>>() {
                                     @Override
                                     public Tuple2<String, Integer> map(
                                             Tuple4<String,String,String,Integer> view)
                                             throws Exception {
                                         return new Tuple2<String, Integer>(
                                                 view.f1,  //User
                                                 view.f3);       //minutes
                                     }
                                 }
                            )

                            //Group by User
                            .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {

                                @Override
                                public String getKey(Tuple2<String, Integer> codeValue)
                                        throws Exception {
                                    return codeValue.f0; //User
                                }
                            })

                            //Create Tumbling window of 5 seconds
                            .window( TumblingProcessingTimeWindows.of(Time.seconds(5)))

                            //Compute Summary and publish results
                            .process(new ProcessWindowFunction<Tuple2<String, Integer>,
                                    Tuple3<String, String,Integer>, String, TimeWindow>() {

                                @Override
                                public void process(String user,
                                                    Context context,
                                                    Iterable<Tuple2<String, Integer>> iterable,
                                                    Collector<Tuple3<String,String,Integer>> collector)
                                        throws Exception {

                                    Integer totalMinutes = 0;
                                    for( Tuple2<String,Integer> userView : iterable ) {
                                        totalMinutes = totalMinutes + userView.f1;
                                    }

                                    collector.collect(new Tuple3<String,String,Integer>(
                                            new Date(context.window().getStart()).toString(),
                                            user,
                                            totalMinutes));

                                }
                            });

            //Print Summary records
            userWindowedSummary.print();

//            //Use a RichSinkFunction to update score by topic
//            viewsData
//                    .addSink(new RichSinkFunction<Tuple4<String,String,String,Integer>>() {
//
//                        RedisManager redisUpdater;
//                        @Override
//                        public void open(Configuration parameters)
//                                throws Exception {
//                            super.open(parameters);
//                            redisUpdater = new RedisManager();
//                            redisUpdater.setUp();
//                        }
//
//                        @Override
//                        public void close() throws Exception {
//                            super.close();
//                        }
//
//                        @Override
//                        public void invoke(Tuple4<String,String,String,Integer> userView,
//                                           Context context)
//                                throws Exception {
//                            //Update score in Redis
//                            redisUpdater.update_score(
//                                    userView.f2, //topic
//                                     1.0); // count 1 per record
//                        }
//                    });

            // execute the streaming pipeline
            streamEnv.execute("Flink Website Views Analytics");

//            final CountDownLatch latch = new CountDownLatch(1);
//            latch.await();
        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }
}
