package rts;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import rts.tasks.CalculateTFIDF;
import rts.tasks.CheckLength;
import rts.tasks.CombinedDescription;
import rts.tasks.ContainsKeyWords;
import rts.tasks.ExtractTimeStamp;
import rts.tasks.FilterNoLabels;
import rts.tasks.IsEnglish;
import rts.tasks.PreProcessing;
import rts.tasks.SelectSummary;
import rts.tasks.TweetParser;
import rts.tasks.Word2VecTest;


public class SumStream {

	public static void main(String[] args) throws Exception
	{
//		set the environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		get the parameters
		ParameterTool params = ParameterTool.fromArgs(args);
	    env.getConfig().setGlobalJobParameters(params);
        
	    
//	   	get twitter data from a json file
		DataStreamSource<String> twitterData = env.readTextFile(params.get("inputJson"));
		
		DataStream<JsonNode> parsedDataWithTopicID = twitterData.map(new TweetParser(params.get("inputTextID"))).filter(new FilterNoLabels());
		    
//		combine the final tweet which contains most improtant features for summarization. e.g. topic id, description, title and assessed label.
		DataStream<Tuple2<String, JsonNode>> finalData =  parsedDataWithTopicID.map(new CombinedDescription(params.get("inputTopic")));
		
//		preprocess the tweets which remove urls, @, hashtags, character repetition, words starting with a number etc. Additionally, create
//		the documents list which contain keys(topics) and the tweets. 
		DataStream<Tuple2<String, JsonNode>> preprocessed = finalData.map(new PreProcessing(params.get("stopWord")))
				.filter(new ContainsKeyWords())
				.filter(new CheckLength())
				.filter(new IsEnglish());
////				create a map of strings which share the same topic id key	
//		
////		DataStream<Tuple2<String, String>> output = preprocessed.keyBy(0).map(new CalculateTFIDF()).flatMap(new SelectSummary());
//		DataStream<String> output = preprocessed.keyBy(0).map(new CalculateTFIDF()).flatMap(new Word2VecTest(params.get("stopWord")));

//		output.writeAsText(params.get("output")).setParallelism(1);
		
//		DataStream<Tuple2<String, JsonNode>> preprocessed = finalData
//				.map(new PreProcessing(params.get("stopWord")))
//				.assignTimestampsAndWatermarks(new ExtractTimeStamp())
//				.keyBy(0);
////				.window(TumblingEventTimeWindows.of(Time.days(1)));
	    env.execute("Twitter Summarization");
	}
}


