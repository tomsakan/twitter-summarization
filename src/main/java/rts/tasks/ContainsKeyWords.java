package rts.tasks;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

public class ContainsKeyWords implements FilterFunction<Tuple2<String, JsonNode>>{

	private static final long serialVersionUID = -3430717343015541539L;

	public boolean filter(Tuple2<String, JsonNode> node){
		ArrayList<String> al= new ArrayList<String>();
		
//		String[] description = node.f1.get("description").asText().toLowerCase().split(" ");
//		String[] narrative = node.f1.get("narrative").asText().toLowerCase().split(" ");
		String[] title = node.f1.get("title").asText().toLowerCase().split(" ");
		
//		for(String word : description) al.add(word);
//		for(String word : narrative) al.add(word);
		for(String word : title) al.add(word);
		
		String tweet = node.f1.get("text").asText().toLowerCase();
		
		return al.parallelStream().anyMatch(tweet::contains);
	}
}