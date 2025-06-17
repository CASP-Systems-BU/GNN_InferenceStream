package sinks;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.slf4j.Logger;


/**
 * A Sink that drops all data and periodically emits latency measurements
 */
public class DummyLatencyCountingSink<T> extends StreamSink<T> {

    private final Logger LOG;
    public String markerID;

    public DummyLatencyCountingSink(Logger log) {
        super(new SinkFunction() {

            @Override
            public void invoke(Object value, Context ctx) {
            }
        });
        LOG = log;
    }

    @Override
    public void processLatencyMarker(LatencyMarker marker) {
        long currentTime=System.currentTimeMillis();
        long lMarkedTime=marker.getMarkedTime();
        LOG.warn("%{}%{}%{}%{}%{}%{}%{}%{}%{}%{}", "latency",
                currentTime-lMarkedTime,
                currentTime,
                lMarkedTime,
                marker.getSubtaskIndex(),
                getRuntimeContext().getIndexOfThisSubtask(),
                marker.getOperatorId()+"_"+marker.getSubtaskIndex(),
                "Sink",
                "processLatencyMarker",
                "latencyFromSink_"+markerID
        );
    }
}
