
package org.example;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.example.TwoHop.AggregateToJsonFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class AggregateToJsonFunctionTest {

    private KeyedOneInputStreamOperatorTestHarness<Integer, Tuple5<Integer, Integer, Integer, float[], Integer>, String> testHarness;
    private AggregateToJsonFunction function;

    @BeforeEach
    public void setup() throws Exception {
        function = new TwoHop.AggregateToJsonFunction();

        // Wrap the function inside KeyedProcessOperator
        KeyedProcessOperator<Integer, Tuple5<Integer, Integer, Integer, float[], Integer>, String> keyedProcessOperator = new KeyedProcessOperator<>(function);
        testHarness = new KeyedOneInputStreamOperatorTestHarness<>(
                keyedProcessOperator,
                (KeySelector<Tuple5<Integer, Integer, Integer, float[], Integer>, Integer>) tuple -> tuple.f0,
                TypeInformation.of(Integer.class)
        );
        testHarness.open();
    }

    @Test
    public void testBuildJsonSinglePass() throws Exception {
        // Input tuples
        List<Tuple5<Integer, Integer, Integer, float[], Integer>> tuples = new ArrayList<>();
        tuples.add(new Tuple5<>(100, 200, 400, new float[]{0.40f, 0.41f}, 2));
        tuples.add(new Tuple5<>(100, 300, 600, new float[]{0.60f, 0.61f}, 2));
        tuples.add(new Tuple5<>(100, 100, 300, new float[]{0.30f, 0.31f}, 1));
        tuples.add(new Tuple5<>(100, 100, 200, new float[]{0.20f, 0.21f}, 1));
        tuples.add(new Tuple5<>(100, 200, 500, new float[]{0.50f, 0.51f}, 2));
        tuples.add(new Tuple5<>(100, 100, 100, new float[]{0.10f, 0.11f}, 0));





        //send tuples to the test harness
        for (Tuple5<Integer, Integer, Integer, float[], Integer> tuple : tuples) {
            testHarness.processElement(tuple, 1L); // Same timestamp for all tuples
        }

        // fire the timer at event_time + 10 to process all
        testHarness.setProcessingTime(11L);


        testHarness.processWatermark(11L);


        // colelcting output and validate
        List<String> results = testHarness.extractOutputValues();

        assertEquals(1, results.size(), "expected exactly 1 subgraph json");
        String actualJson = results.get(0);

        // Expected json
        String expectedJson = "{"
                + "\"data\":["
                + "[0.1,0.11],"
                + "[0.2,0.21],"
                + "[0.4,0.41],"
                + "[0.3,0.31],"
                + "[0.6,0.61],"
                + "[0.5,0.51]"
                + "],"
                + "\"edge_index\":["
                + "[1,3,0,0,1],"
                + "[2,4,3,1,5]"
                + "],"
                + "\"target_node_id\":100"
                + "}";

        assertEquals(expectedJson.replaceAll("\\s", ""), actualJson.replaceAll("\\s", ""),
                "not matched");
    }
}

