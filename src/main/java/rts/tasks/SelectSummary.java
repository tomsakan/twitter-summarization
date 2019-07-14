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

public class SelectSummary implements FlatMapFunction<Tuple2<String, JsonNode>, Tuple2<String, JsonNode>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1587117239129149532L;
	
	SummariesList summaries = new SummariesList();
	
	Integer summarySize = 5;

	@Override
	public void flatMap(Tuple2<String, JsonNode> node, Collector<Tuple2<String, JsonNode>> out) throws Exception {
		// TODO Auto-generated method stub
//		System.out.println(node.f0+" "+node.f1.get("id").asText()+" "+node.f1.get("cosine_score").asText());
//		System.out.println(node.f0);
		if(summaries.getSummary(node.f0) == null){
			summaries.addSummary(node.f0, node.f1.get("id").asText(), node.f1);
		}else{
			if(summaries.getSummarySize(node.f0) >= summarySize){
				boolean moreThanSum = false;
//				HashMap<String, Double> sorted = summaries.getSummariesToSort(node.f0).entrySet().stream()
//						.sorted(comparingByValue())
//						.collect(toMap(HashMap.Entry::getKey, HashMap.Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));
				
//				for(Entry<String, JsonNode> summary : summaries.getSummary(node.f0).entrySet()){
//					double sumCosine = summary.getValue().get("cosine_score").asDouble();
////					String topicTemp = summary.getKey();
//					if(node.f1.get("cosine_score").asDouble() > sumCosine){
//						summaries.addSummary(node.f0, node.f1.get("id").asText(), node.f1);
//						moreThanSum = true;
//						break;
//					}
//				}
			}else{
				summaries.addSummary(node.f0, node.f1.get("id").asText(), node.f1);
			}
		}
		
		
	}
}
