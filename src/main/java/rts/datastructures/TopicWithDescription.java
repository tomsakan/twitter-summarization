package rts.datastructures;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class TopicWithDescription implements Serializable{

	private static final long serialVersionUID = -4947000294986695138L;
	public HashMap<String, String> topicWithDes = new HashMap<String, String>();
	
	public TopicWithDescription() {};
	
	public TopicWithDescription(String path) throws IOException {
		 FileReader file2 = new FileReader(new File(path));
	        BufferedReader read2 = new BufferedReader(file2);
	        String line = null;
	        ObjectMapper jsonParser = new ObjectMapper();
	        while ((line = read2.readLine()) != null) 
	        {
	        	JsonNode node = jsonParser.readValue(line, JsonNode.class);
	        	newItem(node.get("topid").asText(), line);
	        }
	       read2.close();
	}
	
	public void newItem(String topid, String description){
		topicWithDes.put(topid, description);
	}
	
	public String getDescription(String topid){
		if(topicWithDes.containsKey(topid)){
			return topicWithDes.get(topid);
		}else return null;
	}
	
	public HashMap<String, String> getTopicWithDes() {
		return topicWithDes;
	}

	public void setTopicWithDes(HashMap<String, String> topicWithDes) {
		this.topicWithDes = topicWithDes;
	}
}
