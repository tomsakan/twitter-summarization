package rts.datastructures;

import java.io.Serializable;
import java.util.HashMap;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

public class SummariesList implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -316570763694424885L;
	
	public HashMap<String, HashMap<String, JsonNode>> summaries = new HashMap<String, HashMap<String, JsonNode>>();
	
	public HashMap<String, HashMap<String, Double>> summariesToSort = new HashMap<String, HashMap<String, Double>>();
	
	public SummariesList() {};
	
	public void addSummary(String topicID, String id, JsonNode node){
		HashMap<String, JsonNode> innerMap = (HashMap<String, JsonNode>) summaries.get(topicID);
		HashMap<String, Double> innerMapToSort = new HashMap<String, Double>();
		if(innerMap == null){
			innerMap = new HashMap<String, JsonNode>();
			innerMap.put(id, node);
			innerMapToSort.put(id, node.get("cosine_score").asDouble());
			summaries.put(topicID, innerMap);
			summariesToSort.put(topicID, innerMapToSort);
			
		}else{
			innerMap.put(id, node);
			innerMapToSort.put(id, node.get("cosine_score").asDouble());
			summaries.put(topicID, innerMap);
			summariesToSort.put(topicID, innerMapToSort);
		}
	}
	
	public Integer getSummarySize(String topicID){
		return summaries.get(topicID).size();
	}
	
	public HashMap<String, JsonNode> getSummary(String topicID){
		return summaries.get(topicID);
	}
	
	public HashMap<String, Double> getSummariesToSort(String topicID){
		return summariesToSort.get(topicID);
	}
	
	public void removeSummary(String topicID, String id){
		summaries.get(topicID).remove(id);
	}

}
