package org.example;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class TwoHopPipelineBuilder {

    public static DataStream<String> buildTwoHopPipeline(StreamExecutionEnvironment env,
                                                         DataStream<String> inputNodeStream,    //pass in a test source
                                                         int firstHopLimit,
                                                         int secondHopLimit
    ) {
        try {env.setStateBackend(new RocksDBStateBackend("file:///tmp/checkpoints", true));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create RocksDBStateBackend", e);
        }

        env.setParallelism(1);

        // 1) parse node ID from String to Integer
        DataStream<Integer> parsedStream = inputNodeStream
                .map(raw -> { String[] parts = raw.split(",", 2);
                    return Integer.parseInt(parts[0]); // node ID as int
                })
                .name("ExtractNodeId")
                .map(x -> { System.out.println("Parsed node id: " + x); return x; });

        // 2) first-hop
        DataStream<Tuple4<Integer, Integer, Integer, Integer>> firstHopNeighbors = parsedStream
                .keyBy(nodeId -> nodeId)
                .flatMap(new TwoHop.FirstHopFunction(firstHopLimit))
                .returns(TypeInformation.of(new TypeHint<Tuple4<Integer, Integer, Integer, Integer>>() {}))
                .name("FirstHop");

        firstHopNeighbors.print("First-hop neighbors:");

        // 3) second-hop
        DataStream<Tuple4<Integer, Integer, Integer, Integer>> secondHopNeighbors = firstHopNeighbors
                .keyBy(t -> t.f2)
                .flatMap(new TwoHop.SecondHopFunction(secondHopLimit))
                .returns(TypeInformation.of(new TypeHint<Tuple4<Integer, Integer, Integer, Integer>>() {}))
                .name("SecondHop");

        secondHopNeighbors.print("Second-hop neighbors:");

        // 4) add target node
        DataStream<Tuple4<Integer, Integer, Integer, Integer>> addTargetNode = parsedStream
                .map(nodeId -> new Tuple4<>(nodeId, nodeId, nodeId, 0))
                .returns(TypeInformation.of(new TypeHint<Tuple4<Integer, Integer, Integer, Integer>>() {}))
                .name("AddTargetnode");

        addTargetNode.print("Added target node:");

        // 5) union
        DataStream<Tuple4<Integer, Integer, Integer, Integer>> finalMergedStream = firstHopNeighbors.union(secondHopNeighbors).union(addTargetNode);
        finalMergedStream.print("Merged stream:");

        // 6) feature retrieval
        DataStream<Tuple5<Integer, Integer, Integer, float[], Integer>> finalOutput = finalMergedStream
                .keyBy(t -> t.f2)
                .flatMap(new TwoHop.FeatureRetrievalFunction())
                .returns(TypeInformation.of(new TypeHint<Tuple5<Integer, Integer, Integer, float[], Integer>>() {}))
                .name("FeatureRetrieval");

        // 7) aggregate to json
        DataStream<String> jsonSubgraph = finalOutput
                .keyBy(t -> t.f0) // Key by integer
                .process(new TwoHop.AggregateToJsonFunction())
                .returns(TypeInformation.of(new TypeHint<String>() {}))
                .name("AggregateToJson");

        jsonSubgraph.print("Aggregated JSON Output:");

        // 8) replacing torchserve inference with a simple map function
        DataStream<String> inferenceResults = jsonSubgraph
                .map(json -> "MOCK_INF_FOR: " + json)
                .name("MockTorchServe");

        return inferenceResults;
    }
}
