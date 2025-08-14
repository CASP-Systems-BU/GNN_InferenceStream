package org.example;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;
import org.apache.flink.util.CloseableIterator;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TwoHopIntegrationTest {

    // 1) Creatinf a small MiniCluster that can run thejob
    @ClassRule
    public static MiniClusterWithClientResource MINI_CLUSTER = new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build()
            );

    @Test
    public void testFullTwoHopPipeline() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        // input(nodeId, eventTime)
        List<String> inputEvents = List.of("100,1000", "200,2000");


        // 1) WatermarkStrategy that extracts timestamps
        WatermarkStrategy<String> strategy = WatermarkStrategy
                        .<String>forMonotonousTimestamps()  // or forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((element, recordTimestamp) -> {
                            String[] parts = element.split(",", 2);
                            return Long.parseLong(parts[1]);
                        });

        // 2) fromCollection + assignTimestampsAndWatermarks
        var inputStream = env
                .fromCollection(inputEvents) //bounded datastream from input
                .assignTimestampsAndWatermarks(strategy);

        // 3) build the pipeline
        var finalDataStream = TwoHopPipelineBuilder.buildTwoHopPipeline(env, inputStream, 2, 2);

        // 4) collect output
        try (var iterator = finalDataStream.executeAndCollect("TestTwoHopJob")) //all reuslts from inference
        {
            List<String> outputs = new ArrayList<>();
            iterator.forEachRemaining(outputs::add);

            System.out.println("final outputss...");
            outputs.forEach(System.out::println);

            org.junit.Assert.assertFalse("for example get some results", outputs.isEmpty());
        }
    }

}
