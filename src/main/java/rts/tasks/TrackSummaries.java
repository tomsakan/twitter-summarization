package rts.tasks;

import java.util.ArrayList;
import java.util.Collections;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import rts.datastructures.DissimilarityCheck;
import rts.datastructures.SummariesList;

public class TrackSummaries extends ProcessWindowFunction<Tuple2<String, JsonNode>, String, String, TimeWindow>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -4128331456397649116L;
	
	@Override
	public void process(String key,Context context, Iterable<Tuple2<String, JsonNode>> input,Collector<String> out){
		
		SummariesList summaries = new SummariesList();
		DissimilarityCheck disSim = new DissimilarityCheck();
		ArrayList<String> strings = new ArrayList<String>();
		
	    for (Tuple2<String, JsonNode> in: input) {
	    	if(summaries.getSummaries(in.f0) == null){
				summaries.addCosineScore(in.f0, in.f1.get("cosine_score").asDouble());
				disSim.addSummary(in.f0, in.f1.get("text").asText());
//				if(in.f0.equals("RTS48")){
					strings.add(in.f1.get("cosine_score").asText() + "!@" + in.f1.get("created_at").asText() + "!@" + in.f1.get("id").asText() + "!@" + in.f0);
//				}
			}else{
				Double threshold = summaries.getAvergeScore(in.f0);
				if((in.f1.get("cosine_score").asDouble() >= threshold) && in.f1.get("cosine_score").asDouble() != 0.0){
					summaries.addCosineScore(in.f0, in.f1.get("cosine_score").asDouble());

//					if(in.f0.equals("RTS48")){
						if(disSim.checkDissim(in.f0, in.f1.get("text").asText())){
							disSim.addSummary(in.f0, in.f1.get("text").asText());
                            strings.add(in.f1.get("cosine_score").asText() + "!@" + in.f1.get("created_at").asText() + "!@" + in.f1.get("id").asText() + "!@" + in.f0);
						}
//					}
				}else{
					summaries.addCosineScore(in.f0, in.f1.get("cosine_score").asDouble());
				}
			}
	    
	    }
	    
	    Integer rank = 0;
	    Collections.sort(strings, Collections.reverseOrder());   
	    for(String summary : strings){
	    	rank++;
	    	out.collect(SummaryParser(summary, rank));
	    }
	}
	
	public String SummaryParser(String tweet, Integer rank){
		String[] string = tweet.split("!@");
		String date = "";
		String format = "Q0";
		String tweet_id = string[2];
		Double score = Double.parseDouble(string[0]);
		String runtag = "tws";
		String[] time = string[1].split(" "); 
		String month = "";
		String topic_id = string[3];
		
		if(time[1].equals("Jan")) month = "01";
		else if(time[1].equals("Feb")) month = "02";
		else if(time[1].equals("Mar")) month = "03";
		else if(time[1].equals("Apr")) month = "04";
		else if(time[1].equals("May")) month = "05";
		else if(time[1].equals("Jun")) month = "06";
		else if(time[1].equals("Jul")) month = "07";
		else if(time[1].equals("Aug")) month = "08";
		else if(time[1].equals("Sep")) month = "09";
		else if(time[1].equals("Oct")) month = "10";
		else if(time[1].equals("Nov")) month = "11";
		else if(time[1].equals("Dec")) month = "12";
		
		date = time[5]+month+time[2];
		
		return date + " " + topic_id + " " + format + " " + tweet_id + " " + rank + " " + score + " " + runtag ;
	}
}
