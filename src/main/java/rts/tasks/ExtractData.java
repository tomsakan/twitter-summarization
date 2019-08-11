package rts.tasks;

import java.util.HashMap;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

public class ExtractData implements MapFunction<Tuple2<String, JsonNode>, String>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 5956961926993704490L;
	
	HashMap<String, Integer> map = new HashMap<String, Integer>();

	@Override
	public String map(Tuple2<String, JsonNode> node) throws Exception {
		// TODO Auto-generated method stub
		if(map.get(node.f0) == null){
			map.put(node.f0, 1);
		}else{
			map.put(node.f0, map.get(node.f0)+1);
		}
		String date = "";
		String topic_id = node.f0;
		String format = "Q0";
		String tweet_id = node.f1.get("id").asText();
		Integer rank = map.get(node.f0);
		Double score = node.f1.get("cosine_score").asDouble();
		String runtag = "tws";
		String[] time = node.f1.get("created_at").asText().split(" "); 
		String month = "";
		
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
		
//		System.out.println(date + " " + topic_id + " " + format + " " + tweet_id + " " + rank + " " + score + " " + runtag );
		return date + " " + topic_id + " " + format + " " + tweet_id + " " + rank + " " + score + " " + runtag ;
	}

}
