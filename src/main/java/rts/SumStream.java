package rts;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import rts.tasks.CalculateTFIDF;
import rts.tasks.CheckLength;
import rts.tasks.CombinedDescription;
import rts.tasks.TweetParser;
import rts.tasks.FilterNoLabels;
import rts.tasks.PreProcessing;
import rts.tasks.ContainsKeyWords;
import rts.tasks.CreateDocumentList;



public class SumStream {
	
//	static class CosineSimilarityList{
//		public HashMap<String, HashMap<String, Double>> cosineList = new HashMap<String, HashMap<String, Double>>();
//		
//		
//		public void addItem(String topicID, String id, Double cosineSim){
//
//			HashMap<String, Double> insideMap = (HashMap<String, Double>) cosineList.get(topicID);
//			
////			if key is null, create a new map
//			if(insideMap == null){
//				insideMap = new HashMap<String, Double>();
//				insideMap.put(id, cosineSim);
//				cosineList.put(topicID, insideMap);
//			}else{
//				insideMap.put(id, cosineSim);
//				cosineList.put(topicID, insideMap);
//			}
//		}
//		
//		public HashMap<String, Double> getCosineSim(String topicID){
//			if(cosineList.containsKey(topicID)){
//				return cosineList.get(topicID);
//			}else return null;
//		}
//	}

	public static void main(String[] args) throws Exception
	{
//		set the environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		get the parameters
		ParameterTool params = ParameterTool.fromArgs(args);
	    env.getConfig().setGlobalJobParameters(params);
        
//	   	get twitter data from a json file
		DataStreamSource<String> twitterData = env.readTextFile(params.get("inputTest3"));
		DataStream<JsonNode> parsedDataWithTopicID = twitterData.map(new TweetParser("/Users/Tutumm/rt_sum/dataset/input/test1.txt")).filter(new FilterNoLabels());
		    
//		combine the final tweet which contains most improtant features for summarization. e.g. topic id, description, title and assessed label.
		DataStream<Tuple2<String, JsonNode>> finalData =  parsedDataWithTopicID.map(new CombinedDescription("/Users/Tutumm/rt_sum/dataset/input/test2.json"));
		    
//		preprocess the tweets which remove urls, @, hashtags, character repetition, words starting with a number etc. Additionally, create
//		the documents list which contain keys(topics) and the tweets. 
		DataStream<Tuple2<String, JsonNode>> preprocessed = finalData.map(new PreProcessing("/Users/Tutumm/rt_sum/dataset/input/stopwords.txt"))
				.filter(new ContainsKeyWords())
				.filter(new CheckLength());
		
//				create a map of strings which share the same topic id key	
		DataStream<Tuple2<String, JsonNode>> output = preprocessed.keyBy(0).map(new CalculateTFIDF());
		
//		System.out.println(GlobalVar.dict);
//		addDictionary.writeAsText(params.get("output"));
	    env.execute("Twitter Summarization");
	}
}


