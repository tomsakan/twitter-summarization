package rts;

import org.apache.flink.table.sources.TableSource;

import java.io.IOException;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.TreeNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;

public class SumStream {

	public static void main(String[] args) throws Exception
	{
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		ParameterTool params = ParameterTool.fromArgs(args);
	    env.getConfig().setGlobalJobParameters(params);
	    
//	    get twitter data from a json file
	    DataStream<String> twitterData = env.readTextFile(params.get("inputJson"));
	    DataStream<JsonNode> parsedData = twitterData.map(new TweetParser());
	    
//	    get twitter id and topic id from a text file as string
	    DataStream<String> topicWithTweetID = env.readTextFile(params.get("inputTextID"));
	    DataStream<JsonNode> jsonTopicWithTweetID = topicWithTweetID.map(new TopicToJSON());
	    
//	    get title with its ID
	    DataStream<String> titleWithID = env.readTextFile(params.get("inputTopic"));
	    DataStream<JsonNode> jsonTitleWithID = titleWithID.map(new TitleNameToJSON());
	    
	    env.execute("Twitter Summarization");
	}
	
	public static class TweetParser implements MapFunction<String, JsonNode>{
		
		public JsonNode map(String value) throws Exception{
			
			ObjectMapper jsonParser = new ObjectMapper();
			
			JsonNode node = jsonParser.readValue(value, JsonNode.class);
			
			List<String> keepList = Lists.newArrayList("created_at", "id", "full_text");
			
			node = ((ObjectNode) node).retain(keepList);

			return node;
		}
	}
	
	public static class TopicToJSON implements MapFunction<String, JsonNode>{
		
		public JsonNode map(String value) throws Exception{
			ObjectMapper jsonParser = new ObjectMapper();
			
			String[] word = value.split(" ");
			
			JsonNode parsedJson = jsonParser.readTree("{\"topic_id\":\""+word[0]+"\",\"id\":\""+word[2]+"\",\"assessed_label\":\""+word[3]+"\"}");
			
//			System.out.println(parsedJson);
			return parsedJson;
		}
	}
	
	public static class TitleNameToJSON implements MapFunction<String, JsonNode>{
		
		public JsonNode map(String value) throws Exception{
			
			ObjectMapper jsonParser = new ObjectMapper();
			JsonNode node = jsonParser.readValue(value, JsonNode.class);
//			System.out.println(node.get("topid").asText());
			return node;
			
			
		}
	}
	
}


