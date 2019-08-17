package rts.tasks;

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

public class CalculateWord2Vec implements MapFunction<Tuple2<String, JsonNode>, Tuple2<String, JsonNode>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = -3776139154977101494L;
	
	private static ILexicalDatabase db = new NictWordNet();
	private final static String URL_REGEX = "((www\\.[\\s]+)|(https?://[^\\s]+))";
	private final static String CONSECUTIVE_CHARS = "([a-z])\\1{1,}";
	private final static String STARTS_WITH_NUMBER = "[1-9]\\s*(\\w+)";
	List<String> stopwords = null;
	
	public CalculateWord2Vec(String path) throws IOException{
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
	public Tuple2<String, JsonNode> map(Tuple2<String, JsonNode> node) throws Exception {
		JsonNode parsedJson = node.f1;
//		if(node.f0.equals("RTS47") || node.f0.equals("RTS48") || node.f0.equals("RTS49")){
//			System.out.println(node);
			String tweet = regEx(node.f1.get("original_text").asText());
			String description = regEx(node.f1.get("title").asText());
			
//			System.out.println("Label: " + node.f1.get("actual_label").asText() + "\nW2V Score: " + compute(cleanText(tweet), cleanText(description)) + "\nTF-IDF Score: " + node.f1.get("cosine_score").asDouble() + "\nTweet: " + node.f1.get("original_text").asText());
//			System.out.println(node.f0);
			parsedJson = node.f1;
			((ObjectNode) parsedJson).put("word2vec", compute(cleanText(tweet), cleanText(description)));
			return new Tuple2<String, JsonNode>(node.f0, parsedJson);
//		}
//		((ObjectNode) parsedJson).put("word2vec", 0.0);
//		return new Tuple2<String, JsonNode>(node.f0, parsedJson);
	}

}
