package rts.tasks;

import java.io.IOException;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import rts.datastructures.TopicWithDescription;

public class CombinedDescription implements MapFunction<JsonNode, Tuple2<String, JsonNode>>{

	private static final long serialVersionUID = -7560226696338546529L;
	TopicWithDescription td = null;
	
	public CombinedDescription(String tweet2TopicFile) throws IOException {
		td = new TopicWithDescription(tweet2TopicFile);
	}

	public Tuple2<String, JsonNode> map(JsonNode node) throws Exception{
		
		ObjectMapper jsonParser = new ObjectMapper();
		
		JsonNode finalNode = node;
		
		try{
			String line = td.getDescription(node.get("topic_id").asText());
			JsonNode description = jsonParser.readValue(line, JsonNode.class);
			((ObjectNode) finalNode).put("narrative", description.get("narrative").asText());
    		((ObjectNode) finalNode).put("description", description.get("description").asText());
    		((ObjectNode) finalNode).put("title", description.get("title").asText());
    		((ObjectNode) finalNode).put("count", 1);
    		
    		return new Tuple2<String, JsonNode>(finalNode.get("topic_id").asText(), finalNode);
		}catch(Exception e){
			return null;
		}
	}
}