package rts.tasks;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

import rts.datastructures.SummariesList;

public class SelectSummary implements FlatMapFunction<Tuple2<String, JsonNode>, Tuple2<String, String>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1587117239129149532L;
	
	SummariesList summaries = new SummariesList();
	
	Integer summarySize = 5;

	@Override
	public void flatMap(Tuple2<String, JsonNode> node, Collector<Tuple2<String, String>> out) throws Exception {
		
		if(summaries.getSummaries(node.f0) == null){
			summaries.addCosineScore(node.f0, node.f1.get("cosine_score").asDouble());
//			System.out.println(node.f0+"\t"+summaries.getCount(node.f0));
			if(node.f0.equals("RTS48")){
				System.out.println(node.f1.get("actual_label").asText()+"\t"+node.f1.get("original_text").asText());
				out.collect(new Tuple2<String, String>(node.f1.get("original_text").asText(), node.f1.get("actual_label").asText()));
			}
		}else{
			Double threshold = summaries.getAvergeScore(node.f0);
//			Double threshold = 0.4;
			if((node.f1.get("cosine_score").asDouble() >= threshold) && node.f1.get("cosine_score").asDouble() != 0.0){
//				if(node.f0.equals("RTS48")){
//					summaries.addCosineScore(node.f0, node.f1.get("cosine_score").asDouble());
//					System.out.println(summaries.getAvergeScore(node.f0));
//				}
//				System.out.println(node);
				summaries.addCosineScore(node.f0, node.f1.get("cosine_score").asDouble());
//				System.out.println(node.f1.get("actual_label").asText()+"\t"+node.f1.get("cosine_score").asText());
				if(node.f0.equals("RTS48")){
					System.out.println(node.f1.get("actual_label").asText()+"\t"+node.f1.get("original_text").asText());
					out.collect(new Tuple2<String, String>(node.f1.get("original_text").asText(), node.f1.get("actual_label").asText()));
				}
			}else{
				summaries.addCosineScore(node.f0, node.f1.get("cosine_score").asDouble());
//				System.out.println(node.f1.get("actual_label").asText()+"\t"+node.f1.get("cosine_score").asText());
//				System.out.println("Bye");
			}
		}
		
		if(node.f0.equals("RTS47")) out.collect(new Tuple2<String, String>(node.f1.get("original_text").asText(), node.f1.get("actual_label").asText()));
		
		
	}
}
