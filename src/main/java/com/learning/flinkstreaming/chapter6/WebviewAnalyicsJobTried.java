package com.learning.flinkstreaming.chapter6;

import java.util.Date;
import java.util.Properties;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

public class WebviewAnalyicsJobTried {

    public static final String kafkaTopic = "streaming.views.input";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env  =  StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink.streaming.realtime");
        FlinkKafkaConsumer<String> kafkaConsumer =
                new FlinkKafkaConsumer<>(kafkaTopic, new SimpleStringSchema(),
                        properties);

        //Setup to receive only new messages
        kafkaConsumer.setStartFromLatest() ;

        DataStream<String> source = env.addSource(kafkaConsumer);

        DataStream<Tuple4<String, String, String, Integer>> mapped = source.map(new MapFunction<String, Tuple4<String, String, String, Integer>>() {

            @Override
            public Tuple4<String, String, String, Integer> map(String value) throws Exception {
                String[] input = value.split(",");


                return new Tuple4<>(input[1], input[0], input[2], Integer.valueOf(input[3]));
            }
        });
        
        mapped.keyBy(new KeySelector<Tuple4<String, String, String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple4<String, String, String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple4<String, String, String, Integer>,Tuple3<String, String, Integer>, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<Tuple4<String, String, String, Integer>,
                                    Tuple3<String, String, Integer>, String, TimeWindow>.Context context, Iterable<Tuple4<String, String, String, Integer>> elements,
                                                Collector<Tuple3<String, String, Integer>> out) throws Exception {

                                int totalMin = 0;
                                for (Tuple4<String, String, String, Integer> each: elements) {
                                    totalMin = totalMin + each.f3;
                                }
                                out.collect(new Tuple3<String,String, Integer>
                                        (new Date(context.window().getStart()).toString()
                                        , s, totalMin)
                                );
                            }
                        }).print();
                

        env.execute();
    }
}
