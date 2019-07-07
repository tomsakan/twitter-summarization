package rts;

import org.apache.flink.table.sources.TableSource;
import org.apache.flink.util.Collector;

import scala.util.parsing.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.plantranslate.JsonMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.TreeNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.api.java.functions.KeySelector;


public class SumStream {

	static class TopicWithID{
	    public HashMap<String, String>  tweetWithIDs = new HashMap<String, String>();

	    public void newItem(String id, String topicIDandLabel){
	    	tweetWithIDs.put(id, topicIDandLabel);
	    }

	    public String getID(String id){
	        if (tweetWithIDs.containsKey(id)){
	            return tweetWithIDs.get(id);
	        }else{
	        	return null;
	        }
	    }
	}
	
	static class TopicWithDescription{
		public HashMap<String, String> topicWithDes = new HashMap<String, String>();
		
		public void newItem(String topid, String description){
			topicWithDes.put(topid, description);
		}
		
		public String getDescription(String topid){
			if(topicWithDes.containsKey(topid)){
				return topicWithDes.get(topid);
			}else return null;
		}
	}
	
	static class GlobalVar{
		public static TopicWithID ti = new TopicWithID();
		public static TopicWithDescription td = new TopicWithDescription();
	}
	

	public static void main(String[] args) throws Exception
	{
//		set the environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		get the parameters
		ParameterTool params = ParameterTool.fromArgs(args);
	    env.getConfig().setGlobalJobParameters(params);
	    
//	    read file that contains topic id
	    FileReader file1 = new FileReader(new File("/Users/Tutumm/rt_sum/dataset/input/rts2017-batch-qrels.txt"));
        BufferedReader read1 = new BufferedReader(file1);
        String line = null;
        String[] word = null;
        while ((line = read1.readLine()) != null) 
        {
        	word = line.split(" ");
            GlobalVar.ti.newItem(word[2], word[0]+" "+word[3]);
        }
        
//      read file that contains topic description and title
        FileReader file2 = new FileReader(new File("/Users/Tutumm/rt_sum/dataset/input/1.TREC2017-RTS-topics-final.json"));
        BufferedReader read2 = new BufferedReader(file2);
        line = null;
        word = null;
        ObjectMapper jsonParser = new ObjectMapper();
        while ((line = read2.readLine()) != null) 
        {
        	JsonNode node = jsonParser.readValue(line, JsonNode.class);
        	GlobalVar.td.newItem(node.get("topid").asText(), line);
        }
        
//	    	get twitter data from a json file
		    DataStreamSource<String> twitterData = env.readTextFile(params.get("inputJson"));
		    DataStream<JsonNode> parsedDataWithTopicID = twitterData.map(new TweetParser()).filter(new FilterNoLabel());
		    
//		    combine the final tweet which contains most improtant features for summarization. e.g. topic id, description, title and assessed label.
		    DataStream<Tuple2<String, JsonNode>> finalData =  parsedDataWithTopicID.map(new CombinedDescription());
		    
//		    preprocess the tweets which remove urls, @, hashtags, character repetition, words starting with a number etc.
		    DataStream<Tuple2<String, JsonNode>> preprocessed = finalData.map(new PreProcessing())
		    		.filter(new ContainsKeywords())
		    		.filter(new CheckLength());
		    
//		    preprocessed.print();
		    preprocessed.writeAsText(params.get("output"));
//	    
	    env.execute("Twitter Summarization");
	}
	
	public static class TweetParser implements MapFunction<String, JsonNode>{		
		public JsonNode map(String value) throws Exception{
			
			ObjectMapper jsonParser = new ObjectMapper();
			
			JsonNode node = jsonParser.readValue(value, JsonNode.class);
			
			List<String> keepList = Lists.newArrayList("created_at", "id", "full_text");
			
			node = ((ObjectNode) node).retain(keepList);
		   
			JsonNode parsedJson = node;
			
			try{
				String[] idAndLabel = GlobalVar.ti.getID(node.get("id").asText()).split(" ");
	    		((ObjectNode) parsedJson).put("topic_id", idAndLabel[0]);
	    		((ObjectNode) parsedJson).put("assessed_label", idAndLabel[1]);
	    		return parsedJson;
			}catch(Exception e){
				return parsedJson;
			}
		}
	}
	
	public static class FilterNoLabel implements FilterFunction<JsonNode>{
		public boolean filter(JsonNode node){
			if(node.get("assessed_label") != null){
				return true;
			}else return false;
		}
	}

	public static class CombinedDescription implements MapFunction<JsonNode, Tuple2<String, JsonNode>>{
		public Tuple2<String, JsonNode> map(JsonNode node) throws Exception{
			
			ObjectMapper jsonParser = new ObjectMapper();
//			JsonNode node = jsonParser.readValue(node, JsonNode.class);
			
			JsonNode finalNode = node;
			
			try{
				String line = GlobalVar.td.getDescription(node.get("topic_id").asText());
				JsonNode description = jsonParser.readValue(line, JsonNode.class);
				((ObjectNode) finalNode).put("narrative", description.get("narrative").asText());
	    		((ObjectNode) finalNode).put("description", description.get("description").asText());
	    		((ObjectNode) finalNode).put("title", description.get("title").asText());
	    		
	    		return new Tuple2<String, JsonNode>(finalNode.get("topic_id").asText(), finalNode);
			}catch(Exception e){
				return null;
			}
		}
	}

	public static class PreProcessing implements MapFunction<Tuple2<String, JsonNode>, Tuple2<String, JsonNode>>{
		
		private final static String URL_REGEX = "((www\\.[\\s]+)|(https?://[^\\s]+))";
		private final static String CONSECUTIVE_CHARS = "([a-z])\\1{1,}";
		private final static String STARTS_WITH_NUMBER = "[1-9]\\s*(\\w+)";
		
		public Tuple2<String, JsonNode> map(Tuple2<String, JsonNode> node) throws Exception{
			String tweet = node.f1.get("full_text").asText();
			
//			remove urls
			tweet = tweet.replaceAll(URL_REGEX, "");
			
//			remove username
			tweet = tweet.replaceAll("@([^\\s]+)", "");
			
//			remove character repetition
			tweet = tweet.replaceAll(CONSECUTIVE_CHARS, "$1");
			
//			remove words starting with a number
			tweet = tweet.replaceAll(STARTS_WITH_NUMBER, "");
			
//			remove hashtags
			tweet = tweet.replaceAll("#[A-Za-z]+","");
			
			tweet = tweet.replaceAll("[^\\p{L}\\p{M}\\p{N}\\p{P}\\p{Z}\\p{Cf}\\p{Cs}\\s]", "");
			
			JsonNode preprocessedJson = node.f1;
    		((ObjectNode) preprocessedJson).put("text",tweet);
			
			return new Tuple2<String, JsonNode>(node.f0, preprocessedJson);
		}
	}

	public static class ContainsKeywords implements FilterFunction<Tuple2<String, JsonNode>>{
		public boolean filter(Tuple2<String, JsonNode> node){
			ArrayList<String> al= new ArrayList<String>();
			
			String[] description = node.f1.get("description").asText().toLowerCase().split(" ");
			String[] narrative = node.f1.get("narrative").asText().toLowerCase().split(" ");
			String[] title = node.f1.get("title").asText().toLowerCase().split(" ");
			
			for(String word : description) al.add(word);
			for(String word : narrative) al.add(word);
			for(String word : title) al.add(word);
			
			String tweet = node.f1.get("text").asText().toLowerCase();
			
			return al.parallelStream().anyMatch(tweet::contains);
		}
	}

	public static class CheckLength implements FilterFunction<Tuple2<String, JsonNode>>{
		public boolean filter(Tuple2<String, JsonNode> node){
			return node.f1.get("text").asText().length() >= 10;
		}
	}
}


