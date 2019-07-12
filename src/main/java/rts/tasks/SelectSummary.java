package rts.tasks;

import java.io.Serializable;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

public class SelectSummary implements FlatMapFunction<Tuple2<String, JsonNode>, Tuple2<String, JsonNode>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1587117239129149532L;

	@Override
	public void flatMap(Tuple2<String, JsonNode> node, Collector<Tuple2<String, JsonNode>> out) throws Exception {
		// TODO Auto-generated method stub
		System.out.println(node.f0+" "+node.f1.get("id").asText()+" "+node.f1.get("cosine_score").asText());
		
	}
}
