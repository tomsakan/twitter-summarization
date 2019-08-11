package rts;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import org.apache.flink.util.Collector;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;

import rts.tasks.CalculateTFIDF;
import rts.tasks.CheckLength;
import rts.tasks.CombinedDescription;
import rts.tasks.ContainsKeyWords;
import rts.tasks.ExtractData;
import rts.tasks.ExtractTimeStamp;
import rts.tasks.FilterNoLabels;
import rts.tasks.IsEnglish;
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
//				.map(new PreProcessing(params.get("stopWord")))
//				.filter(new ContainsKeyWords())
//				.filter(new CheckLength())
//				.filter(new IsEnglish())
//				.map(new CalculateTFIDF())
//				.map(new ExtractData())
				.assignTimestampsAndWatermarks(new ExtractTimeStamp());
		
		DataStream<String> out = windowedData.map(new Test1())
				.keyBy(new KeySelector<Tuple3<String, JsonNode, Integer>, String>(){
					/**
					 * 
					 */
					private static final long serialVersionUID = -7344657668381199842L;

					public String getKey(Tuple3<String, JsonNode, Integer> value)
					{	
//						System.out.println(value.f0);
						return value.f0;
					}
				})
				.window(TumblingEventTimeWindows.of(Time.days(1)))
//				.reduce(new Test());
				.process(new TrackSummaries());
		
//		DataStream<Tuple2<String, JsonNode>> out = finalData
//				.map(new PreProcessing(params.get("stopWord")))
////				.assignTimestampsAndWatermarks(new ExtractTimeStamp())
//				.keyBy(new KeySelector<Tuple2<String, JsonNode>, String>(){
//					/**
//					 * 
//					 */
//					private static final long serialVersionUID = -7344657668381199842L;
//
//					public String getKey(Tuple2<String, JsonNode> value)
//					{	
////						System.out.println(value.f0);
//						return value.f0;
//					}
//				})
//				.window(TumblingEventTimeWindows.of(Time.days(1)))
//				.process(new TrackSummaries());
//				.reduce(new Test());
		
		out.writeAsText(params.get("output")).setParallelism(1);
	    env.execute("Twitter Summarization");
	}
}


