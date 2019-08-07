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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
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

public class Word2VecTest implements FlatMapFunction<Tuple2<String, JsonNode>, Tuple2<String, String>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = -8961767159042322291L;
	private static ILexicalDatabase db = new NictWordNet();
	private final static String URL_REGEX = "((www\\.[\\s]+)|(https?://[^\\s]+))";
	private final static String CONSECUTIVE_CHARS = "([a-z])\\1{1,}";
	private final static String STARTS_WITH_NUMBER = "[1-9]\\s*(\\w+)";
	List<String> stopwords = null;
	
	public Word2VecTest(String path) throws IOException{
		stopwords = Files.readAllLines(Paths.get(path));
	}
	
	public double[][] getSimilarityMatrix( String[] words1, String[] words2, RelatednessCalculator rc )
	{
	    double[][] result = new double[words1.length][words2.length];
	    for ( int i=0; i<words1.length; i++ ){
	        for ( int j=0; j<words2.length; j++ ) {
	            double score = rc.calcRelatednessOfWords(words1[i], words2[j]);
	            result[i][j] = score;
	          }
	        }
	    return result;
	  }

	private void compute (String[] tweet, String[] description)
	{
//		System.out.println("WuPalmer");
	    RelatednessCalculator rc1 = new WuPalmer(db);
	    double[][] s1 = getSimilarityMatrix(tweet, description,rc1);
	    for(int i=0; i<tweet.length; i++){
	    	for(int j=0; j< description.length; j++){
	    		System.out.println(tweet[i] + " - " + description[j] + " " +s1[i][j]);
	    	} 
//	    	System.out.println();
	    }
	  }
	
	@Override
	public void flatMap(Tuple2<String, JsonNode> node, Collector<Tuple2<String, String>> out) throws Exception {
		if(node.f0.equals("RTS48")){
			
			String tweet = node.f1.get("original_text").asText();
			String description = node.f1.get("original_description").asText();
			
			tweet = tweet.replaceAll(URL_REGEX, "");
			tweet = tweet.replaceAll("@([^\\s]+)", "");
			tweet = tweet.replaceAll("[^\\p{IsDigit}\\p{IsAlphabetic}]", " ");
			tweet = tweet.replaceAll("#[A-Za-z]+","");
			tweet = tweet.replaceAll("\n", "");
			tweet = tweet.replaceAll("RT", "");
			description = description.replaceAll("[^\\p{IsDigit}\\p{IsAlphabetic}]", " ");
			description = description.replaceAll("#[A-Za-z]+","");
			
			ArrayList<String> filteredWords = (ArrayList) Stream.of(tweet.toLowerCase().split(" ")).collect(Collectors.toCollection(ArrayList<String>::new));
			filteredWords.removeAll(stopwords);
			tweet = filteredWords.stream().collect(Collectors.joining(" "));
			
			filteredWords = (ArrayList) Stream.of(description.toLowerCase().split(" ")).collect(Collectors.toCollection(ArrayList<String>::new));
			filteredWords.removeAll(stopwords);
			description = filteredWords.stream().collect(Collectors.joining(" "));
			
			compute(tweet.split(" "), description.split(" "));
			System.out.println("---------------------------------------------");
//			int count = 0;
//			double score = 0.0;
//			double max = 0.0;
//			
//			
//			double sim = score/count;
//			System.out.println("Label: " + node.f1.get("actual_label").asText() + "\nSimilarity Score: " + sim + "\nTweet: " + node.f1.get("original_text").asText());
			
//        out.collect(new Tuple2<String, String>(node.f1.get("original_text").asText(), node.f1.get("actual_label").asText()));
		}
	}

}
