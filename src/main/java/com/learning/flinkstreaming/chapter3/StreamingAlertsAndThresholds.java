package com.learning.flinkstreaming.chapter3;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


/****************************************************************************
 * This example is an example for Streaming Alerts & Thresholds in Flink
 * It reads a real time exception stream from Kafka,
 * compares with thresholds and publishes alerts to outgoing topics.
 ****************************************************************************/

public class StreamingAlertsAndThresholds {


    public static void main(String[] args){

        try {

            System.out.println("******** Initiating Streaming Alerts and Thresholds *************");

            // Set up the streaming execution environment
            final StreamExecutionEnvironment streamEnv
                    = StreamExecutionEnvironment.getExecutionEnvironment();

            //Initiate the Kafka alerts Generator
            KafkaAlertsDataGenerator alertsGenerator = new KafkaAlertsDataGenerator();
            Thread genThread = new Thread(alertsGenerator);
            genThread.start();

            //Set connection properties to Kafka Cluster
            Properties srcProps = new Properties();
            srcProps.setProperty("bootstrap.servers", "localhost:9092");
            srcProps.setProperty("group.id", "flink.streaming.realtime");

            //Setup a Kafka Consumer on Flnk
            FlinkKafkaConsumer<String> kafkaConsumer =
                    new FlinkKafkaConsumer<>
                            ("streaming.alerts.input", //topic
                                    new SimpleStringSchema(),
                                    srcProps); //connection properties

            //Setup to receive only new messages
            kafkaConsumer.setStartFromLatest() ;

            //Create the data stream
            DataStream<String> alertsRawInput =
                    streamEnv.addSource(kafkaConsumer);

            //Extract information from CSV String
            DataStream<Tuple4<String,String,String,String>> alertsData =
                    alertsRawInput
                    .map(new MapFunction<String, Tuple4<String, String, String, String>>() {
                        @Override
                        public Tuple4<String, String, String, String> map(String csvInput)
                                throws Exception {

                            System.out.println("Received Data: " + csvInput);

                            String[] alertArray = csvInput
                                                    .replace("\"","")
                                                    .split(",");
                            String timestamp = alertArray[0];
                            String level = alertArray[1];
                            String code = alertArray[2];
                            String message = alertArray[3];

                            return new Tuple4<String,String,String,String>
                                    (timestamp,level,code,message);
                        }
                    });


            //Define a Kafka Sink for CRITICAL Alerts
            Properties destProps = new Properties();
            destProps.setProperty("bootstrap.servers", "localhost:9092");

            FlinkKafkaProducer<Tuple4<String,String,String,String>> criticalProducer =
                    new FlinkKafkaProducer<Tuple4<String,String,String,String>>(
                            "streaming.alerts.critical",
                            new KafkaSerializationSchema<Tuple4<String, String, String, String>>() {

                                @Override
                                public ProducerRecord<byte[], byte[]>
                                serialize(Tuple4<String, String, String, String> alert,
                                          @Nullable Long aLong) {

                                    String key = alert.f0;
                                    String value =
                                            "\"" + alert.f0 + "\"," +
                                                    "\"" + alert.f1 + "\"," +
                                                    "\"" + alert.f2 + "\"," +
                                                    "\"" + alert.f3 + "\"," ;

                                    return new ProducerRecord<byte[],byte[]>(
                                            "streaming.alerts.critical",
                                            value.getBytes());
                                }
                            },
                            destProps,
                            FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

            //Extract only CRITICAL Alerts and send to the critical topic.
            alertsData
                    .filter(new FilterFunction<Tuple4<String, String, String, String>>() {
                        @Override
                        public boolean filter(Tuple4<String, String, String, String> alert)
                                throws Exception {

                            return alert.f1.equals("CRITICAL");
                        }
                    })
                    .addSink(criticalProducer);


            //Compute codewise total count by 10 seconds
            DataStream<Tuple3<String,String,Long>> codeWindowedSummary =
                    alertsData
                            //Extract Code and count
                            .map(new MapFunction<Tuple4<String,String,String,String>,
                                         Tuple2<String, Long>>() {
                                     @Override
                                     public Tuple2<String, Long> map(
                                             Tuple4<String,String,String,String> alert)
                                             throws Exception {
                                         return new Tuple2<String, Long>(
                                                 alert.f2,  //Code
                                                 1L);       //Count of 1
                                     }
                                 }
                            )

                            //Group by Code
                            .keyBy(new KeySelector<Tuple2<String, Long>, String>() {

                                @Override
                                public String getKey(Tuple2<String, Long> codeValue)
                                        throws Exception {
                                    return codeValue.f0;
                                }
                            })

                            //Create Tumbling window of 10 seconds
                            .window( TumblingProcessingTimeWindows.of(Time.seconds(10)))

                            //Compute Summary and publish results
                            .process(new ProcessWindowFunction<Tuple2<String, Long>,
                                    Tuple3<String, String,Long>, String, TimeWindow>() {

                                @Override
                                public void process(String code,
                                                    Context context,
                                                    Iterable<Tuple2<String, Long>> iterable,
                                                    Collector<Tuple3<String,String,Long>> collector)
                                        throws Exception {

                                    Long totalValue = 0L;
                                    for( Tuple2<String,Long> codeAlert : iterable ) {
                                        totalValue = totalValue + codeAlert.f1;
                                    }
                                    collector.collect(new Tuple3<String,String,Long>(
                                            new Date(context.window().getStart()).toString(),
                                            code,
                                            totalValue));

                                    System.out.println("Summary :" +
                                            " Date : " + context.window().getStart() +
                                            ", Code : " + code +
                                            ", Total : " + totalValue );

                                }
                            });

            //Create Flink producer for high volume alerts
            FlinkKafkaProducer<Tuple3<String,String,Long>> highVolumeProducer =
                    new FlinkKafkaProducer<Tuple3<String,String,Long>>(
                            "streaming.alerts.highvolume",
                            new KafkaSerializationSchema<Tuple3<String, String, Long>>() {

                                @Override
                                public ProducerRecord<byte[], byte[]>
                                serialize(Tuple3<String, String, Long> alert,
                                          @Nullable Long aLong) {

                                    String key = alert.f0;
                                    String value =
                                            "\"" + alert.f0 + "\"," +
                                                    "\"" + alert.f1 + "\"," +
                                                    "\"" + alert.f2 + "\"," ;

                                    System.out.println("Publishing High Volume Alert :" +
                                            value);

                                    return new ProducerRecord<byte[],byte[]>(
                                            "streaming.alerts.highvolume",
                                            value.getBytes());
                                }
                            },
                            destProps,
                            FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

            //Check if volume exceeds threshold and publish to highvolume topic.
            codeWindowedSummary
                    .filter(new FilterFunction<Tuple3<String, String, Long>>() {
                        @Override
                        public boolean filter(Tuple3<String, String, Long> codeSummary)
                                throws Exception {
                            //If count more than 2 in 10 seconds for the code.
                            return codeSummary.f2 > 2L;
                        }
                    })
                    .addSink(highVolumeProducer);

            // execute the streaming pipeline
            streamEnv.execute("Flink Alerts and Thresholds");

            final CountDownLatch latch = new CountDownLatch(1);
            latch.await();
        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }
}
