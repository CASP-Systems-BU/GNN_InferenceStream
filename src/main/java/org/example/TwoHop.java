package org.example;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.shaded.zookeeper3.com.codahale.metrics.Timer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import sinks.DummyLatencyCountingSink;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.json.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.stream.IntStream;

////This program fetches the neighborhoods (2 hop neighbors+features) of the stream of target nodes (through kafka source)
////by accessing state in rocksDB (This is populated through the dataloader program which we run and take a savepoint of)
//// It then constructs a json subgraph for each target node to be sent to a pytorch gnn model deployed on torchserve
////This model returns us the embeddings of the target nodes and we write them to a kafka sink

public class TwoHop {
    private static final Logger logger = LoggerFactory.getLogger(TwoHop.class);

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: TwoHop <auth_token> <first_hop_limit> <second_hop_limit>");
            return;
        }
        final String authToken = args[0];
        final int firstHopLimit = Integer.parseInt(args[1]);
        final int secondHopLimit = Integer.parseInt(args[2]);
        final int nodesPerBurst = Integer.parseInt(args[3]);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(16);
        env.getConfig().setLatencyTrackingInterval(2000L); // emits latency markers every 2 seconds


        // Synthetic node IDs as source
        DataStream<String> rawStream = env.addSource(new SyntheticNodeSource(nodesPerBurst))
                .name("Synthetic Node Source")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<String>forBoundedOutOfOrderness(Duration.ofMillis(100))
                                .withTimestampAssigner((record, ts) -> {
                                    String[] parts = record.split(",", 2);
                                    return Long.parseLong(parts[1]);
                                })
                ).setParallelism(1);

        DataStream<Integer> parsedStream = rawStream
                .map(raw -> Integer.parseInt(raw.split(",", 2)[0]))
                .name("ExtractNodeId");
        String checkpointDir = "file://" + System.getProperty("user.dir") + "/checkpoints";

        env.setStateBackend(new RocksDBStateBackend(checkpointDir, true));

        env.disableOperatorChaining();

        // 5) First hop
        DataStream<Tuple4<Integer,Integer,Integer,Integer>> firstHopNeighbors = parsedStream
                .keyBy(nodeId -> nodeId)
                .flatMap(new FirstHopFunction(firstHopLimit))
                .uid("stateProcessor1").name("FirstHop");

        // 6) Second hop
        DataStream<Tuple4<Integer,Integer,Integer,Integer>> secondHopNeighbors = firstHopNeighbors
                .keyBy(t -> t.f2)
                .flatMap(new SecondHopFunction(secondHopLimit))
                .returns(TypeInformation.of(new TypeHint<Tuple4<Integer,Integer,Integer,Integer>>(){}))
                .uid("stateProcessor2").name("SecondHop");


        // 7) Add target node as hop=0
        DataStream<Tuple4<Integer,Integer,Integer,Integer>> addTargetNode = parsedStream
                .map(nodeId -> new Tuple4<>(nodeId, nodeId, nodeId, 0))
                .returns(TypeInformation.of(new TypeHint<Tuple4<Integer,Integer,Integer,Integer>>(){}))
                .uid("addTargetNode").name("AddTargetnode");

        // 8) Union
        DataStream<Tuple4<Integer,Integer,Integer,Integer>> finalMergedStream =
                firstHopNeighbors
                        .union(secondHopNeighbors)
                        .union(addTargetNode);

        // 9) Feature retrieval
        DataStream<Tuple5<Integer,Integer,Integer,float[],Integer>> finalOutput = finalMergedStream
                .keyBy(t -> t.f2)
                .flatMap(new FeatureRetrievalFunction())
                .returns(TypeInformation.of(new TypeHint<Tuple5<Integer,Integer,Integer,float[],Integer>>(){}))
                .uid("stateProcessor3").name("FeatureRetrieval");

        // 10) Aggregate to JSON
        DataStream<String> jsonSubgraph = finalOutput
                .keyBy(t -> t.f0)
                .process(new AggregateToJsonFunction())
                .name("AggregateToJson")
                .uid("aggregateToJson")
                .setParallelism(60);

        // 11) TorchServe inference (Async)
        DataStream<String> inferenceResults = AsyncDataStream.unorderedWait(
                jsonSubgraph,
                new TorchServeAsyncFunction(authToken),
                60, TimeUnit.SECONDS,
                2  // capacity
        ).name("TorchServeInference");

        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class); // Can process any type of data, needed by the transform operator

        DummyLatencyCountingSink<String> sinker = new DummyLatencyCountingSink<>(logger); // Instance of logging latency
        sinker.markerID = "Q3";

        // Attach the latency counting sink
        inferenceResults.transform("Q3_Sink", objectTypeInfo, sinker)
                .setParallelism(1)
                .slotSharingGroup("sink");

        // Kafka sink properties
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Kafka sink
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("inference-results-topic")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();

        // Add Kafka sink to inferenceResults stream
        inferenceResults
                .sinkTo(kafkaSink)
                .name("Kafka Sink for Inference Results")
                .setParallelism(1)
                .slotSharingGroup("sink");

        // Execute
        env.execute("TwoHop");


    }

    public static class FirstHopFunction extends RichFlatMapFunction<Integer, Tuple4<Integer,Integer,Integer,Integer>> {

        private transient ListState<Integer> neighborsState;
        private static final Logger logger = LoggerFactory.getLogger(FirstHopFunction.class);
        private final int firstHopLimit;

        public FirstHopFunction(int firstHopLimit) { this.firstHopLimit = firstHopLimit; }

        @Override
        public void open(Configuration parameters) throws Exception {
            neighborsState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("neighbors", Integer.class));
        }

        @Override
        public void flatMap(Integer nodeId, Collector<Tuple4<Integer,Integer,Integer,Integer>> out) throws Exception {
            List<Integer> neighborIdsList = new ArrayList<>();
            for (Integer nbr : neighborsState.get()) {
                neighborIdsList.add(nbr);
            }

            List<Integer> limitedNeighbors = neighborIdsList.stream()
                    .limit(firstHopLimit)
                    .collect(Collectors.toList());


            for (Integer nbr : limitedNeighbors) {
                out.collect(new Tuple4<>(nodeId, nodeId, nbr, 1));
            }

            int remaining = firstHopLimit - limitedNeighbors.size();
            for (int i=0; i<remaining; i++) {
                // Instead of sending negative IDs to aggregator
                int dummyNeighbor = -1*(i+1);
                out.collect(new Tuple4<>(nodeId, nodeId, dummyNeighbor, 1));
            }
        }
    }

    //SecondHopFunction
    public static class SecondHopFunction extends RichFlatMapFunction<
            Tuple4<Integer,Integer,Integer,Integer>,
            Tuple4<Integer,Integer,Integer,Integer>> {

        private transient ListState<Integer> neighborsState2;
        private final int secondHopLimit;
        private static final Logger logger = LoggerFactory.getLogger(SecondHopFunction.class);

        public SecondHopFunction(int secondHopLimit) { this.secondHopLimit = secondHopLimit; }

        @Override
        public void open(Configuration parameters) throws Exception {
            neighborsState2 = getRuntimeContext().getListState(new ListStateDescriptor<>("neighbors2", Integer.class));
        }

        @Override
        public void flatMap(Tuple4<Integer,Integer,Integer,Integer> value,
                            Collector<Tuple4<Integer,Integer,Integer,Integer>> out) throws Exception {
            Integer targetNode = value.f0;
            Integer firstHopNbr = value.f2;

            List<Integer> neighbors = new ArrayList<>();
            for (Integer n : neighborsState2.get()) {
                neighbors.add(n);
            }

            List<Integer> limited = neighbors.stream()
                    .limit(secondHopLimit)
                    .collect(Collectors.toList());

            for (Integer secondHopNbr : limited) {
                out.collect(new Tuple4<>(targetNode, firstHopNbr, secondHopNbr, 2));
            }

            int remain = secondHopLimit - limited.size();
            for (int i=0; i<remain; i++) {
                int dummy = -1*(i+1);
                out.collect(new Tuple4<>(targetNode, firstHopNbr, dummy, 2));
            }
        }
    }


    public static class FeatureRetrievalFunction
            extends RichFlatMapFunction< Tuple4<Integer,Integer,Integer,Integer>,
            Tuple5<Integer,Integer,Integer,float[],Integer> > {

        private transient ValueState<float[]> featuresState3;
        private static final Logger logger = LoggerFactory.getLogger(FeatureRetrievalFunction.class);

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<float[]> desc = new ValueStateDescriptor<>(
                    "featuresState3", TypeInformation.of(new TypeHint<float[]>() {}));
            featuresState3 = getRuntimeContext().getState(desc);
        }

        @Override
        public void flatMap(Tuple4<Integer,Integer,Integer,Integer> value,
                            Collector<Tuple5<Integer,Integer,Integer,float[],Integer>> out) throws Exception {
            Integer targetNode = value.f0;
            Integer sourceNode = value.f1;
            Integer neighborId = value.f2;
            int hopCount = value.f3;

            float[] feats = (featuresState3.value() == null)
                    ? new float[602]
                    : featuresState3.value();


            out.collect(new Tuple5<>(targetNode, sourceNode, neighborId, feats, hopCount));
        }
    }

    public static class TorchServeAsyncFunction extends RichAsyncFunction<String,String> {
        private static final Logger LOG = LoggerFactory.getLogger(TorchServeAsyncFunction.class);

        private final String authToken;
        private final String urlString = "http://127.0.0.1:8080/predictions/gnnmodeltraced2";
        private transient HttpClient httpClient;

        public TorchServeAsyncFunction(String authToken) {
            this.authToken = "Bearer " + authToken;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.httpClient = HttpClient.newBuilder()
                    .version(HttpClient.Version.HTTP_1_1)
                    .build();
        }

        @Override
        public void asyncInvoke(String jsonInput, ResultFuture<String> resultFuture) throws Exception {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(urlString))
                    .header("Authorization", authToken)
                    .header("Content-Type", "application/json; utf-8")
                    .header("Accept", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonInput))
                    .build();

            httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .whenComplete((response, throwable) -> {
                        if (throwable != null) {
                            LOG.error("Error in async HTTP req", throwable);
                            resultFuture.complete(Collections.singletonList("exception: "+throwable.getMessage()));
                            return;
                        }
                        int code = response.statusCode();
                        if (code==200) {
                            resultFuture.complete(Collections.singletonList(response.body()));
                        } else {
                            LOG.error("HTTP failed code: {}, body={}", code, response.body());
                            resultFuture.complete(Collections.singletonList("rqst failed, code="+code));
                        }
                    });
        }

        @Override
        public void timeout(String input, ResultFuture<String> resultFuture) throws Exception {
            resultFuture.complete(Collections.singletonList("timeout: " + input));
        }
    }


public static class AggregateToJsonFunction
        extends KeyedProcessFunction<Integer,
        Tuple5<Integer, Integer, Integer, float[], Integer>,
        String> {

    private static final Logger logger = LoggerFactory.getLogger(AggregateToJsonFunction.class);

    private transient ListState<Tuple5<Integer, Integer, Integer, float[], Integer>> tuplesState;
    private transient ValueState<Long> targetNodeEventTimeState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ListStateDescriptor<Tuple5<Integer, Integer, Integer, float[], Integer>> desc =
                new ListStateDescriptor<>("tuplesState",
                        TypeInformation.of(new TypeHint<Tuple5<Integer, Integer, Integer, float[], Integer>>() {}));
        tuplesState = getRuntimeContext().getListState(desc);

        ValueStateDescriptor<Long> vd = new ValueStateDescriptor<>("targetNodeEventTime", Long.class);
        targetNodeEventTimeState = getRuntimeContext().getState(vd);
    }

    @Override
    public void processElement(Tuple5<Integer, Integer, Integer, float[], Integer> value, Context ctx, Collector<String> out) throws Exception {

        // Filter out negative dummy neighbors
        if (value.f2 < 0) {
            return;
        }

        tuplesState.add(value);

        if (value.f4 == 0 && targetNodeEventTimeState.value() == null) {
            long eventTime = ctx.timestamp();
            long timerTime = eventTime + 1;
            ctx.timerService().registerEventTimeTimer(timerTime);
            targetNodeEventTimeState.update(eventTime);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        // Convert state to list
        List<Tuple5<Integer, Integer, Integer, float[], Integer>> allTuples = new ArrayList<>();
        for (Tuple5<Integer, Integer, Integer, float[], Integer> t : tuplesState.get()) {
            allTuples.add(t);
        }

        if (allTuples.isEmpty()) {
            //No tuples in aggregator state for this key
            return;
        }


        String subgraph = buildJsonSingleLoop(allTuples,ctx.getCurrentKey());
        if (subgraph != null) {
            out.collect(subgraph);
        }

        tuplesState.clear();
        targetNodeEventTimeState.clear();
    }
    //example tuples
    //        (100, 200, 400, [0.40, 0.41], 2)
    //        (100, 300, 600, [0.60, 0.61], 2)
    //        (100, 100, 300, [0.30, 0.31], 1)
    //        (100, 100, 200, [0.20, 0.21], 1)
    //        (100, 200, 500, [0.50, 0.51], 2)
    //        (100, 100, 100, [0.10, 0.11], 0)

    //nodeIdToIndex = {} — map to track node ID → feature array idx
    //nodeFeatures = [] — stores feature arrays at corresponding indices
    //connections = {} — maps source id to list of neighbor node ids
    //edges = [] — stores the edges as {sourceIndex, targetIndex}.
    //targetNodeId = null

    private String buildJsonSingleLoop(List<Tuple5<Integer, Integer, Integer, float[], Integer>> allTuples, Integer keyedTargetNodeId) throws Exception {
        Map<Integer, Integer> nodeIdToIndex = new HashMap<>();
        List<float[]> nodeFeatures = new ArrayList<>();
        Map<Integer, List<Integer>> connections = new HashMap<>();
        Integer targetNodeId = keyedTargetNodeId != null ? keyedTargetNodeId : null;

        // we'll collect edges here and build them at the end
        List<int[]> edges = new ArrayList<>();
        nodeIdToIndex.put(keyedTargetNodeId, 0);
        nodeFeatures.add(null);


        // process all tuples in a single loop
        for (Tuple5<Integer, Integer, Integer, float[], Integer> tuple : allTuples) {
            int sourceId = tuple.f1;
            int nodeId = tuple.f2;
            float[] features = tuple.f3;
            int hopLevel = tuple.f4;

            // process the target node (hop = 0)
            if (hopLevel == 0) {
                targetNodeId = nodeId;
                if (!nodeIdToIndex.containsKey(nodeId)) {
                    logger.info("HERE in nodidtoindex", nodeIdToIndex);
                    nodeIdToIndex.put(nodeId, 0); // target node gets idx 0
                    nodeFeatures.add(features);
                } else {
                    // update features if we've seen this node before
                    nodeFeatures.set(nodeIdToIndex.get(nodeId), features);
                }
                continue; // node edges to process for target node
            }

            // process src node if not seen before
            if (!nodeIdToIndex.containsKey(sourceId)) {

                nodeIdToIndex.put(sourceId, nodeFeatures.size());
                nodeFeatures.add(null);
                connections.put(sourceId, new ArrayList<>());
            }

            if (!nodeIdToIndex.containsKey(nodeId)) {
                nodeIdToIndex.put(nodeId, nodeFeatures.size());
                nodeFeatures.add(features);
                connections.put(nodeId, new ArrayList<>());
            } else if (features != null) {
                // update features if we've seen this node before
                nodeFeatures.set(nodeIdToIndex.get(nodeId), features);
            }

            // store the connection
            connections.computeIfAbsent(sourceId, k -> new ArrayList<>()).add(nodeId);

            // create an edge
            int sourceIndex = nodeIdToIndex.get(sourceId);
            int targetIndex = nodeIdToIndex.get(nodeId);
            edges.add(new int[]{sourceIndex, targetIndex});

        }

        if (targetNodeId == null) {
            return null; // No target node found
        }

        // Build json directly without additional loops
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();

        //data array
        ArrayNode dataArray = mapper.createArrayNode();
        for (float[] features : nodeFeatures) {
            if (features == null) {
                features = new float[0];
            }
            ArrayNode featureArray = dataArray.addArray();
            for (float f : features) {
                featureArray.add(f);
            }
        }

        root.set("data", dataArray);

        // build edge_index array
        ArrayNode sourceArr = mapper.createArrayNode();
        ArrayNode targetArr = mapper.createArrayNode();
        for (int[] edge : edges) {
            sourceArr.add(edge[0]);
            targetArr.add(edge[1]);
        }
        ArrayNode edgeIndex = mapper.createArrayNode();
        edgeIndex.add(sourceArr);
        edgeIndex.add(targetArr);
        root.set("edge_index", edgeIndex);

        // add target node ID
        root.put("target_node_id", targetNodeId);

        return mapper.writeValueAsString(root);
    }
}



}