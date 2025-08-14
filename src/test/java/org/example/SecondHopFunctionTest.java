package org.example;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.example.TwoHop.SecondHopFunction;
import org.junit.Test;
import static org.junit.Assert.*;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

/**
 * Unit test for the SecondHopFunction.
 * This test verifies that the function correctly retrieves secondhop nbrs
 */
public class SecondHopFunctionTest {

    @Test
    public void testSecondHopLimit2() throws Exception {
        // Create operator with limit=2
        SecondHopFunction function = new SecondHopFunction(2);

        // Create harness
        KeyedOneInputStreamOperatorTestHarness<Integer, Tuple4<Integer, Integer, Integer, Integer>, Tuple4<Integer, Integer, Integer, Integer>> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new StreamFlatMap<>(function),
                        value -> value.f2, // Key selector (use first-hop neighbor as key)
                        BasicTypeInfo.INT_TYPE_INFO
                );

        harness.open();

        // (1) Process a tuple to set key context (simulate a first-hop neighbor)
        Tuple4<Integer, Integer, Integer, Integer> firstHopTuple = new Tuple4<>(100, 100, 201, 1);
        harness.processElement(firstHopTuple, 0L);

        // Clear any initial output before updating state
        harness.getOutput().clear();

        // (2) Reflectively set neighborsState2 for 201
        Field neighborsField = SecondHopFunction.class.getDeclaredField("neighborsState2");
        neighborsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ListState<Integer> neighborsState2 = (ListState<Integer>) neighborsField.get(function);
        // giving three nbrs; the function should limit this to 2
        neighborsState2.update(List.of(301, 302, 303));

        // (3) Process the same first-hop tuple again
        harness.processElement(firstHopTuple, 100L);
        // Unwrap the StreamRecords
        ConcurrentLinkedQueue<Object> rawOutput = harness.getOutput();
        List<Tuple4<Integer, Integer, Integer, Integer>> results = rawOutput.stream().map(record -> ((StreamRecord<Tuple4<Integer, Integer, Integer, Integer>>) record).getValue()).collect(Collectors.toList());
//        for (Object o : rawOutput) {
//            @SuppressWarnings("unchecked")
//            StreamRecord<Tuple4<String, String, String, Integer>> sr =
//                    (StreamRecord<Tuple4<String, String, String, Integer>>) o;
//
//            Tuple4<String, String, String, Integer> t = sr.getValue();
//            results.add(t);
//        }

        // Print all emitted tuples
        System.out.println("Emitted Tuples:");
        results.forEach(System.out::println);

        // Expect only 2 second-hop neighbors
        assertEquals(2, results.size());
        List<Tuple4<Integer, Integer, Integer, Integer>> expected = List.of(
                new Tuple4<>(100, 201, 301, 2),
                new Tuple4<>(100, 201, 302, 2)
        );
        assertTrue("Output contains all expected secondhop nbrs", results.containsAll(expected));

        harness.close();
    }
}
