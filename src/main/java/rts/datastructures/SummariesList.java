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
	
	public SummariesList() {};
	
	public void addSummary(String topicID, String id, JsonNode node){
		HashMap<String, JsonNode> innerMap = (HashMap<String, JsonNode>) summaries.get(topicID);
		if(innerMap == null){
			innerMap = new HashMap<String, JsonNode>();
			innerMap.put(id, node);
			summaries.put(topicID, innerMap);
		}else{
			innerMap.put(id, node);
			summaries.put(topicID, innerMap);
		}
	}
	
	public Integer getSummarySize(String topicID){
		return summaries.get(topicID).size();
	}
	
	public HashMap<String, JsonNode> getSummary(String topicID){
		return summaries.get(topicID);
	}
	
	public void removeSummary(String topicID, String id){
		summaries.get(topicID).remove(id);
	}

}
