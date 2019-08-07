package rts.tasks;

import java.io.IOException;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import rts.datastructures.TopicWithID;


public class TweetParser implements MapFunction<String, JsonNode>{		
	

	private static final long serialVersionUID = -5624155554196593055L;
	TopicWithID ti = null;
	
	public TweetParser(String tweet2TopicFile) throws IOException {
		ti = new TopicWithID(tweet2TopicFile);
	}
	
	public JsonNode map(String value) throws Exception{
		
		ObjectMapper jsonParser = new ObjectMapper();
		
		JsonNode node = jsonParser.readValue(value, JsonNode.class);
		
//		List<String> keepList = Lists.newArrayList("created_at", "id", "full_text");
		
//		node = ((ObjectNode) node).retain(keepList);
	   
		JsonNode parsedJson = node;
		
		try{
			String[] idAndLabel = ti.getID(node.get("id").asText()).split(" ");
    		((ObjectNode) parsedJson).put("topic_id", idAndLabel[0]);
    		((ObjectNode) parsedJson).put("assessed_label", idAndLabel[1]);
    		return parsedJson;
		}catch(Exception e){
			return parsedJson;
		}
	}
}
