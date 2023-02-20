/*
 * *****************************************************
 * Copyright (C) 2022 bytedance.com. All Rights Reserved
 * This file is part of bytedance EA project.
 * Unauthorized copy of this file, via any medium is strictly prohibited.
 * Proprietary and Confidential.
 * ****************************************************
 */

package com.github.cnkeeper.flink.jobs.file;

import com.github.cnkeeper.flink.jobs.Transaction;
import java.io.File;
import java.util.Objects;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

/**
 * FraudDetectionJob
 *
 * @author LeiLi.Zhang <mailto:zhangleili@bytedance.com>
 * @date 2023/1/31
 */


public class FileSourceJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        String file = params.get("file", "src/main/resources/data");

        DataStream<Tuple2<Long, Double>> sumStream = env.readTextFile(file)
            .map(row -> {
                String[] split = row.split(",");
                if (split.length != 2) {
                    return null;
                }
                return new Transaction(Long.parseLong(split[0]), System.currentTimeMillis(),
                    Double.parseDouble(split[1]));
            }).filter(Objects::nonNull)
            .map(new MapFunction<Transaction, Tuple2<Long, Double>>() {
                @Override
                public Tuple2<Long, Double> map(Transaction transaction) throws Exception {
                    return new Tuple2<>(transaction.getAccountId(), transaction.getAmount());
                }
            })
            .keyBy(value -> value.f0)
            .sum(1);

        sumStream.print().setParallelism(1);


        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        JobExecutionResult executionResult = env.execute("FileSourceJob");
        System.out.println(executionResult);
    }
}
