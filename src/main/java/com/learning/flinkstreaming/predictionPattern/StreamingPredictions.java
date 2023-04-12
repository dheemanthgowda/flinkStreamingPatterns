package com.learning.flinkstreaming.predictionPattern;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/****************************************************************************
 * This example is an example for Streaming Predictions in Flink.
 * It reads a real time movie reviews stream from kafka
 * and predicts sentiment of the review
 ****************************************************************************/
public class StreamingPredictions {
    public static void main(String[] args){

        try {

            System.out.println("******** Initiating Streaming Predictions *************");

            // Set up the streaming execution environment
            final StreamExecutionEnvironment streamEnv
                    = StreamExecutionEnvironment.getExecutionEnvironment();

            //Initiate the Kafka Reviews Data Generator
            KafkaReviewsDataGenerator reviewsGenerator = new KafkaReviewsDataGenerator();
            Thread genThread = new Thread(reviewsGenerator);
            genThread.start();;

            //Set connection properties to Kafka Cluster
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", "localhost:9092");
            properties.setProperty("group.id", "flink.streaming.realtime");

            //Setup a Kafka Consumer on Flnk
            FlinkKafkaConsumer<WebsiteReview> kafkaConsumer =
                    new FlinkKafkaConsumer<WebsiteReview>
                            ("streaming.sentiment.input", //topic
                                    new KafkaDeserializationSchema<WebsiteReview>() {
                                        @Override
                                        public TypeInformation<WebsiteReview> getProducedType() {
                                            return TypeInformation.of(WebsiteReview.class);
                                        }
                                        @Override
                                        public boolean isEndOfStream(WebsiteReview ps) {
                                            return false;
                                        }

                                        @Override
                                        public WebsiteReview deserialize(
                                                ConsumerRecord<byte[], byte[]> consumerRecord)
                                                throws Exception {

                                            WebsiteReview review = new WebsiteReview();
                                            review.setId(new String(consumerRecord.key()));
                                            review.setReview(new String(consumerRecord.value()));
                                            review.setSentiment("Not-known");

                                            System.out.println("Received Review: " + review.toString());

                                            return review;
                                        }
                                    },
                                    properties); //connection properties


            //Setup to receive only new messages
            kafkaConsumer.setStartFromLatest() ;

            //Create the data stream
            DataStream<WebsiteReview> reviewInput = streamEnv
                    .addSource(kafkaConsumer);


            //Find sentiment for each review
            DataStream<WebsiteReview> reviewOutput =
                    reviewInput.map(new MapFunction<WebsiteReview, WebsiteReview>() {
                        @Override
                        public WebsiteReview map(WebsiteReview websiteReview)
                                throws Exception {

                            websiteReview.setSentiment(
                                    SentimentPredictor
                                            .getSentiment(websiteReview.review)
                            );
                            System.out.println("Updated Sentiment: "
                                    + websiteReview.toString());
                            return websiteReview;
                        }
                    });

            //Define a Kafka Sink for publishing sentiments
            Properties destProps = new Properties();
            destProps.setProperty("bootstrap.servers", "localhost:9092");

            FlinkKafkaProducer<WebsiteReview> sentimentProducer =
                    new FlinkKafkaProducer<WebsiteReview>(
                            "streaming.sentiment.output",
                            new KafkaSerializationSchema<WebsiteReview>() {

                                @Override
                                public ProducerRecord<byte[], byte[]>
                                serialize(WebsiteReview review,
                                          @Nullable Long aLong) {

                                    String key = review.getId();
                                    String value =review.getSentiment();

                                    return new ProducerRecord<byte[],byte[]>(
                                            "streaming.sentiment.output",
                                            key.getBytes(),
                                            value.getBytes());
                                }
                            },
                            destProps,
                            FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

            reviewOutput.addSink(sentimentProducer);

            // execute the streaming pipeline
            streamEnv.execute("Streaming Predictions");

            final CountDownLatch latch = new CountDownLatch(1);
            latch.await();
        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }
}
