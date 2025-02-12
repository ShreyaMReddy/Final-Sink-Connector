package com.example.window;

import com.typesafe.config.Config;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Optional windowing support that can be used alongside or instead of Elasticsearch's bulk operations.
 * This class doesn't modify the existing ElasticsearchSinkFunction, but provides an additional way
 * to process data in windows.
 */
public class WindowedElasticsearchSink {
    private static final Logger LOG = LoggerFactory.getLogger(WindowedElasticsearchSink.class);

    public static WindowedStream<String, String, TimeWindow> createWindowedStream(
            DataStream<String> inputStream,
            Config config) {
        
        int windowSize = config.getInt("window.size.minutes");
        int slideSize = config.getInt("window.slide.minutes");

        return inputStream
            .keyBy(event -> "global") // Simple global window for now
            .window(SlidingProcessingTimeWindows.of(
                Time.minutes(windowSize),
                Time.minutes(slideSize)
            ));
    }
}
