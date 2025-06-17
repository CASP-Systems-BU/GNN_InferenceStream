package org.example;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class DataLoader {

    private static final Logger LOG = LoggerFactory.getLogger(DataLoader.class);
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(16);

        RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(
                "file:///savepoints", false);
        rocksDBStateBackend.setDbStoragePath("/rocksdb-storage");
        String filePath = "scripts/toyedgesints.csv"; //example file format
                                                        // src node, neighbor 1; neighbor 2, features of src node
                                                        // with features being 602 long

        //file Source for continuous monitoring
        FileSource<String> fileSource = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path(filePath))
                .monitorContinuously(Duration.ofSeconds(10))
                .build();

        DataStream<String> input = env.fromSource(
                fileSource,
                WatermarkStrategy.noWatermarks(),
                "file-source"
        ).uid("source");

        //parsing: Integer node IDs, Integer neighbors, float[] features
        DataStream<Tuple2<Integer, Tuple2<Set<Integer>, float[]>>> parsedData = input
                .filter(line -> !line.startsWith("source_node")) // skip CSV header
                .map(line -> {
                    String[] fields = line.split(",", 3);
                    int nodeId = Integer.parseInt(fields[0]); // Convert source_node to Integer

                    Set<Integer> neighbors = Arrays.stream(fields[1].isEmpty() ? new String[0] : fields[1].split(";"))
                            .map(Integer::parseInt)
                            .collect(Collectors.toSet());

                    // Convert feature string to Float[]
                    Float[] featureArray = Arrays.stream(fields[2].split(","))
                            .map(Float::parseFloat)
                            .toArray(Float[]::new);

                    //convert Float[] to primitive float[]
                    float[] features = new float[featureArray.length];
                    for (int i = 0; i < featureArray.length; i++) {
                        features[i] = featureArray[i]; // unboxing Float to float
                    }

                    return new Tuple2<>(nodeId, new Tuple2<>(neighbors, features));
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<Integer, Tuple2<Set<Integer>, float[]>>>() {}))
                .uid("parser");


        // First UID: Processing edges for neighbor state
        parsedData.keyBy(value -> value.f0)
                .process(new KeyedProcessFunction<Integer, Tuple2<Integer, Tuple2<Set<Integer>, float[]>>, String>() {

                    private transient ListState<Integer> neighborsState;
                    private transient ValueState<float[]> featuresState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ListStateDescriptor<Integer> neighborsDescriptor = new ListStateDescriptor<>(
                                "neighbors", TypeInformation.of(new TypeHint<Integer>() {}));
                        neighborsState = getRuntimeContext().getListState(neighborsDescriptor);

                        ValueStateDescriptor<float[]> featuresDescriptor = new ValueStateDescriptor<>(
                                "features", TypeInformation.of(new TypeHint<float[]>() {}));
                        featuresState = getRuntimeContext().getState(featuresDescriptor);
                    }

                    @Override
                    public void processElement(Tuple2<Integer, Tuple2<Set<Integer>, float[]>> value, Context ctx, Collector<String> out) throws Exception {
                        neighborsState.clear();
                        for (Integer neighbor : value.f1.f0) {
                            neighborsState.add(neighbor);
                        }

                        featuresState.update(value.f1.f1);

                        String result = "Node: " + value.f0 + ", Neighbors: " + value.f1.f0 + ", Features: " + Arrays.toString(value.f1.f1);
                        out.collect(result);
                    }
                }).uid("stateProcessor1").setParallelism(12);

        // Second UID: Processing edges for neighbor state with a different UID
        parsedData.keyBy(value -> value.f0)
                .process(new KeyedProcessFunction<Integer, Tuple2<Integer, Tuple2<Set<Integer>, float[]>>, String>() {

                    private transient ListState<Integer> neighborsState2;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ListStateDescriptor<Integer> neighborsDescriptor = new ListStateDescriptor<>(
                                "neighbors2", TypeInformation.of(new TypeHint<Integer>() {}));
                        neighborsState2 = getRuntimeContext().getListState(neighborsDescriptor);
                    }

                    @Override
                    public void processElement(Tuple2<Integer, Tuple2<Set<Integer>, float[]>> value, Context ctx, Collector<String> out) throws Exception {
                        // Update neighbors state
                        Set<Integer> uniqueNeighbors = new HashSet<>();
                        for (Integer neighbor : neighborsState2.get()) {
                            uniqueNeighbors.add(neighbor);
                        }
                        for (Integer neighbor : value.f1.f0) {
                            if (uniqueNeighbors.add(neighbor)) {
                                neighborsState2.add(neighbor);
                            }
                        }

                        String result = "Node: " + value.f0 + " neighbors: " + uniqueNeighbors;
                        out.collect(result);
                    }
                }).uid("stateProcessor2").setParallelism(12);

        // Third UID: Storing features for both source and destination nodes
        DataStream<Tuple2<Integer, float[]>> featureStream = parsedData
                .map((Tuple2<Integer, Tuple2<Set<Integer>, float[]>> value) -> {
                    // Only emit the source node and its unique features
                    return new Tuple2<>(value.f0, value.f1.f1.clone());
                }).returns(TypeInformation.of(new TypeHint<Tuple2<Integer, float[]>>() {}))
                .uid("featureExtractor");

        featureStream.keyBy(value -> value.f0)
                .process(new KeyedProcessFunction<Integer, Tuple2<Integer, float[]>, Void>() {
                    private transient ValueState<float[]> featuresState3;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<float[]> featuresDescriptor = new ValueStateDescriptor<>(
                                "featuresState3", TypeInformation.of(new TypeHint<float[]>() {}));
                        featuresState3 = getRuntimeContext().getState(featuresDescriptor);
                    }

                    @Override
                    public void processElement(Tuple2<Integer, float[]> value, Context ctx, Collector<Void> out) throws Exception {
                        // Update feature state
                        featuresState3.update(value.f1);
                        LOG.info("Assigned features for node {}: {}", value.f0, Arrays.toString(value.f1));
                    }
                }).uid("stateProcessor3").setParallelism(12);

        env.execute("DataLoader");
    }
}
