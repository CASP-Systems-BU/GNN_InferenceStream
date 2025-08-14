package org.example;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.example.TwoHop.FirstHopFunction;
import org.junit.Test;
import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
/**
 * Demonstrates testing TwoHop.FirstHopFunction in Flink 1.18+ (where setKeyContext is removed).
 *
 * 1) We process an initial "A" element so the harness sets the key context to "A".
 * 2) We reflectively set neighborsState for "A".
 * 3) We process "A" again to produce the actual output.
 */
public class FirstHopFunctionTest {

    @Test
    public void testFirstHopLimit2() throws Exception {
        // Create operator with limit=2
        FirstHopFunction function = new FirstHopFunction(2); //2, 1st hop nbrs

        // create harness
        //single keyed input stream
        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Tuple4<Integer, Integer, Integer, Integer>> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new StreamFlatMap<>(function), //wrapper for flatmap
                        value -> value, //extract key
                        BasicTypeInfo.INT_TYPE_INFO
                );
        harness.open();

        // (1) Trigger the harness to set key context to a (stateful ops work with a key)
        harness.processElement(100, 0L);

        // We don't want the dummy neighbors from the first call, so weclear them
        harness.getOutput().clear();

        // (2) java reflection to access private field neighborstate
        Field neighborsField = FirstHopFunction.class.getDeclaredField("neighborsState"); //gets metdadata for this
        neighborsField.setAccessible(true); //allow modification
        @SuppressWarnings("unchecked")
        ListState<Integer> neighborsState = (ListState<Integer>) neighborsField.get(function);
        neighborsState.update(List.of(101, 102, 103)); //update neighborstate


        // (3) now we process A and expect to find 2 nbrs
        harness.processElement(100, 100L);

        // Unwrap the StreamRecords
        ConcurrentLinkedQueue<Object> rawOutput = harness.getOutput(); //store emitted records in queue , StreamRecord(Tuple4("A", "A", "N1", 1))
        List<Tuple4<String, String, String, Integer>> results = new ArrayList<>();
        for (Object o : rawOutput) {
            @SuppressWarnings("unchecked")
            StreamRecord<Tuple4<String, String, String, Integer>> sr = (StreamRecord<Tuple4<String, String, String, Integer>>) o;

            Tuple4<String, String, String, Integer> t = sr.getValue();
            results.add(t);
        }

        // Print all emitted tuples
        System.out.println("Emitted Tuples:");
        for (Tuple4<String, String, String, Integer> r : results) {
            System.out.println(r);
        }

        List<Tuple4<Integer, Integer, Integer, Integer>> expected = List.of(
                new Tuple4<>(100, 100, 101, 1),
                new Tuple4<>(100, 100, 102, 1)
        );
        assertTrue("Output contains all expected neighbors", results.containsAll(expected));


        harness.close();
    }
}



