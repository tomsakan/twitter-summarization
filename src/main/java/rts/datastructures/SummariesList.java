package rts.datastructures;

import java.io.Serializable;
import java.util.HashMap;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

public class SummariesList implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -316570763694424885L;
	
	public HashMap<String, String> summaries = new HashMap<String, String>();
	
	public SummariesList() {};
	
	public void addCosineScore(String topicID, Double cosine_score){
		if(summaries.get(topicID) == null){
//			summaries = new HashMap<String, String>();
//			System.out.println(topicID);
			summaries.put(topicID, Double.toString(cosine_score)+","+"1");
		}else{
			String temp[] = summaries.get(topicID).split(",");
//			System.out.println(Double.parseDouble(temp[0])*Integer.parseInt(temp[1]));
			Double avgCosine = ((Double.parseDouble(temp[0])*Integer.parseInt(temp[1])) + cosine_score) / (Integer.parseInt(temp[1])+1);
			summaries.put(topicID, Double.toString(avgCosine)+","+(Integer.parseInt(temp[1])+1));
		}
	}
	
	public Double getAvergeScore(String topicID){
		return Double.parseDouble(summaries.get(topicID).split(",")[0]);
	}
	
	public Integer getCount(String topicID){
		return Integer.parseInt(summaries.get(topicID).split(",")[1]);
	}
	
	public String getSummaries(String topicID){
		return summaries.get(topicID);
	}
	
//	public Integer getSummarySize(String topicID){
//		return summaries.get(topicID).size();
//	}
//	
//	public HashMap<String, JsonNode> getSummary(String topicID){
//		return summaries.get(topicID);
//	}
//	
//	public HashMap<String, Double> getSummariesToSort(String topicID){
//		return summariesToSort.get(topicID);
//	}
//	
//	public void removeSummary(String topicID, String id){
//		summaries.get(topicID).remove(id);
//	}

}
