package rts.tasks;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TrackSummaries extends ProcessWindowFunction<Tuple3<String, JsonNode, Integer>, String, String, TimeWindow>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -4128331456397649116L;
	
	@Override
	public void process(String key,Context context, Iterable<Tuple3<String, JsonNode, Integer>> input,Collector<String> out){
		long count = 0;
		String topic = "";
		String date = "";
	    for (Tuple3<String, JsonNode, Integer> in: input) {
//	    	System.out.println(in.f0+"..");
	    	topic = in.f0;
	    	date = in.f1.get("created_at").asText();
	    	count++;
	    }
	    out.collect("Time: " + date + " Topic: " + topic + " count: " + count);
	}

}
