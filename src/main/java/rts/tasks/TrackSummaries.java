package rts.tasks;

import java.util.ArrayList;

import java.util.Collections;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import rts.datastructures.DissimilarityCheck;
import rts.datastructures.SummariesList;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.util.Collector;

import edu.cmu.lti.lexical_db.ILexicalDatabase;
import edu.cmu.lti.lexical_db.NictWordNet;
import edu.cmu.lti.ws4j.RelatednessCalculator;
import edu.cmu.lti.ws4j.impl.HirstStOnge;
import edu.cmu.lti.ws4j.impl.JiangConrath;
import edu.cmu.lti.ws4j.impl.LeacockChodorow;
import edu.cmu.lti.ws4j.impl.Lesk;
import edu.cmu.lti.ws4j.impl.Lin;
import edu.cmu.lti.ws4j.impl.Path;
import edu.cmu.lti.ws4j.impl.Resnik;
import edu.cmu.lti.ws4j.impl.WuPalmer;
import edu.cmu.lti.ws4j.util.WS4JConfiguration;


public class TrackSummaries extends ProcessWindowFunction<Tuple2<String, JsonNode>, String, String, TimeWindow>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -4128331456397649116L;
	private static ILexicalDatabase db = new NictWordNet();
	private final static String URL_REGEX = "((www\\.[\\s]+)|(https?://[^\\s]+))";
	private final static String CONSECUTIVE_CHARS = "([a-z])\\1{1,}";
	private final static String STARTS_WITH_NUMBER = "[1-9]\\s*(\\w+)";
	List<String> stopwords = null;
	
	public TrackSummaries(String path) throws IOException{
		stopwords = Files.readAllLines(Paths.get(path));
	}
	
	public double[][] getSimilarityMatrix( String[] tweet, String[] description, RelatednessCalculator rc ){
	    double[][] result = new double[tweet.length][description.length];
	    for ( int i=0; i<tweet.length; i++ ){
	        for ( int j=0; j<description.length; j++ ) {
	        	if(!tweet[i].equals(description[j])){
	        		double score = rc.calcRelatednessOfWords(tweet[i], description[j]);
		            result[i][j] = score;
	        	}
	          }
	        }
	    return result;
	  }
	
	private double compute (String[] tweet, String[] description){
		int count = 0;
		double score = 0.0;
		double max = 0.0;
	    
		RelatednessCalculator rc1 = new WuPalmer(db);
	    double[][] distance = getSimilarityMatrix(tweet, description,rc1);
	    for(int i=0; i<tweet.length; i++){
	    	for(int j=0; j< description.length; j++){
	    		if(!tweet[i].equals(description[j])){
	    			if(distance[i][j]>max) max = distance[i][j];
//	    			System.out.println(tweet[i] + " - " + description[j] + " " +distance[i][j]);
	    		}
	    	}
	    	count++;
			score = score + max;
	    }
	    return score/count;
	  }
	
private String regEx(String str){
		
		str = str.replaceAll(URL_REGEX, "");
		str = str.replaceAll("@([^\\s]+)", "");
		str = str.replaceAll("[^\\p{IsDigit}\\p{IsAlphabetic}]", " ");
		str = str.replaceAll("#[A-Za-z]+","");
		str = str.replaceAll("\n", "");
		str = str.replaceAll("RT", "");
		
		ArrayList<String> filteredWords = (ArrayList) Stream.of(str.toLowerCase().split(" ")).collect(Collectors.toCollection(ArrayList<String>::new));
		filteredWords.removeAll(stopwords);
		str = filteredWords.stream().collect(Collectors.joining(" "));
		
		return str;
		
	}

private String[] cleanText(String str){
	
	List<String> temp = new ArrayList<>(Arrays.asList(str.split(" ")));
	List<String> terms = new ArrayList<String>();
	
	for(String term : temp){
		if(!(terms.contains(term) || term.equals(" ") || term.equals(""))){
			terms.add(term);
		}
	}
	
	String[] words = new String[terms.size()];
	words = terms.toArray(words);
	
	return words;
	
}

	
	@Override
	public void process(String key,Context context, Iterable<Tuple2<String, JsonNode>> input,Collector<String> out){
		
		SummariesList summaries = new SummariesList();
		DissimilarityCheck disSim = new DissimilarityCheck();
		ArrayList<String> strings = new ArrayList<String>();
		String topic = "";
		
	    for (Tuple2<String, JsonNode> in: input) {
//	    	if(in.f0.equals("RTS47") || in.f0.equals("RTS48") || in.f0.equals("RTS49")){
	    		topic = in.f0;
		    	if(summaries.getSummaries(in.f0) == null){
					summaries.addCosineScore(in.f0, in.f1.get("cosine_score").asDouble());
					disSim.addSummary(in.f0, in.f1.get("text").asText());
						strings.add(in.f1.get("cosine_score").asText() + "!@" + in.f1.get("created_at").asText() + "!@" + in.f1.get("id").asText() + "!@" + in.f0);
				}else{
					Double threshold = summaries.getAvergeScore(in.f0);
//					Double threshold = 0.3;
					if((in.f1.get("cosine_score").asDouble() >= threshold) && in.f1.get("cosine_score").asDouble() != 0.0){
						summaries.addCosineScore(in.f0, in.f1.get("cosine_score").asDouble());
						if(disSim.checkDissim(in.f0, in.f1.get("text").asText())){
							disSim.addSummary(in.f0, in.f1.get("text").asText());
//							String tweet = regEx(in.f1.get("original_text").asText());
//							String description = regEx(in.f1.get("original_description").asText());
//							Double w2vScore = compute(cleanText(tweet), cleanText(description));
//							if(w2vScore > 0.8)
							Double w2vScore = 0.0;
								strings.add((in.f1.get("cosine_score").asDouble() + w2vScore)/2 + "!@" + in.f1.get("created_at").asText() + "!@" + in.f1.get("id").asText() + "!@" + in.f0);
						}	
					}else{
						summaries.addCosineScore(in.f0, in.f1.get("cosine_score").asDouble());
					}
				}
//	    	}
	    
	    }
	    
//	    System.out.println(topic);
	    Integer rank = 0;
	    Collections.sort(strings, Collections.reverseOrder());   
	    for(String summary : strings){
	    	rank++;
	    	out.collect(SummaryParser(summary, rank));
	    }
	}
	
	public String SummaryParser(String tweet, Integer rank){
//		System.out.println(tweet);
		String[] string = tweet.split("!@");
		String date = "";
		String format = "Q0";
		String tweet_id = string[2];
		Double score = Double.parseDouble(string[0]);
		String runtag = "tws";
		String[] time = string[1].split(" "); 
		String month = "";
		String topic_id = string[3];
		
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
		
		return date + " " + topic_id + " " + format + " " + tweet_id + " " + rank + " " + score + " " + runtag;
	}
}
