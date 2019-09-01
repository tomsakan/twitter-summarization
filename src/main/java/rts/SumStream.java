package rts;


import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;


import rts.tasks.CalculateTFIDF;
import rts.tasks.CalculateWord2Vec;
import rts.tasks.CheckFollowers;
import rts.tasks.CheckLength;
import rts.tasks.CombinedDescription;
import rts.tasks.ContainsKeyWords;
import rts.tasks.ExtractData;
import rts.tasks.ExtractTimeStamp;
import rts.tasks.FilterNoLabels;
import rts.tasks.IsEnglish;
import rts.tasks.IsVerified;
import rts.tasks.PreProcessing;
import rts.tasks.SelectSummary;
import rts.tasks.Test;
import rts.tasks.Test1;
import rts.tasks.TrackSummaries;
import rts.tasks.TweetParser;
import rts.tasks.Word2VecTest;


public class SumStream {

	public static void main(String[] args) throws Exception
	{
//		set the environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
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
//		DataStream<Tuple2<String, JsonNode>> preprocessed = finalData.map(new PreProcessing(params.get("stopWord")))
//				.filter(new ContainsKeyWords())
//				.filter(new CheckLength())
//				.filter(new IsEnglish());
//////				create a map of strings which share the same topic id key	
////		
//		DataStream<String> output = preprocessed.keyBy(0).map(new CalculateTFIDF()).flatMap(new SelectSummary()).map(new ExtractData());
//		DataStream<String> output = preprocessed.keyBy(0).map(new CalculateTFIDF()).flatMap(new Word2VecTest(params.get("stopWord")));

//		output.writeAsText(params.get("output")).setParallelism(1);
		
		DataStream<Tuple2<String, JsonNode>> windowedData = finalData
				.map(new PreProcessing(params.get("stopWord")))
				.filter(new ContainsKeyWords())
//				.filter(new CheckLength())
//				.filter(new IsEnglish())
//				.filter(new IsVerified())
//				.filter(new CheckFollowers())
				.map(new CalculateTFIDF())
//				.map(new CalculateWord2Vec(params.get("stopWord")))
//				.flatMap(new SelectSummary())
//				.map(new ExtractData())
				.assignTimestampsAndWatermarks(new ExtractTimeStamp());
		
		DataStream<String> out = windowedData
				.keyBy(new KeySelector<Tuple2<String, JsonNode>, String>(){
					/**
					 * 
					 */
					private static final long serialVersionUID = -7344657668381199842L;

					public String getKey(Tuple2<String, JsonNode> value)
					{	
						return value.f0;
					}
				})
				.window(TumblingEventTimeWindows.of(Time.days(1)))
				.process(new TrackSummaries(params.get("stopWord")));
		
//		out.writeAsText(params.get("output")).setParallelism(1);
	    env.execute("Twitter Summarization");
	}
}


