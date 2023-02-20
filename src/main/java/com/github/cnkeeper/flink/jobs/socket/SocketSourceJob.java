/*
 * *****************************************************
 * Copyright (C) 2022 bytedance.com. All Rights Reserved
 * This file is part of bytedance EA project.
 * Unauthorized copy of this file, via any medium is strictly prohibited.
 * Proprietary and Confidential.
 * ****************************************************
 */

package com.github.cnkeeper.flink.jobs.socket;

import java.util.Objects;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;

/**
 * FraudDetectionJob
 *
 * @author LeiLi.Zhang <mailto:zhangleili@bytedance.com>
 * @date 2023/1/31
 */


public class SocketSourceJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.socketTextStream("localhost", 9999)
            .flatMap(new FlatMapFunction<String, String>() {
                @Override
                public void flatMap(String value, Collector<String> out) throws Exception {
                    if (StringUtils.isNullOrWhitespaceOnly(value)) {
                        return;
                    }
                    String[] words = value.split(",");
                    for (String word : words) {
                        out.collect(word);
                    }
                }
            }).filter(Objects::nonNull)
            .map(new MapFunction<String, Tuple2<String, Long>>() {
                @Override
                public Tuple2<String, Long> map(String value) throws Exception {
                    return Tuple2.of(value, 1L);
                }
            })
            .keyBy(value -> value.f0)
            .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
            .sum(1)
            .print();

        env.execute("SocketSourceJob");
    }
}
