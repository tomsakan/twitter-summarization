package rts.datastructures;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import edu.cmu.lti.lexical_db.ILexicalDatabase;
import edu.cmu.lti.lexical_db.NictWordNet;
import edu.cmu.lti.ws4j.RelatednessCalculator;
import edu.cmu.lti.ws4j.impl.WuPalmer;

public class DissimCheckW2V implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -2968634199360684901L;
	private static ILexicalDatabase db = new NictWordNet();
	private final static String URL_REGEX = "((www\\.[\\s]+)|(https?://[^\\s]+))";
	private final static String CONSECUTIVE_CHARS = "([a-z])\\1{1,}";
	private final static String STARTS_WITH_NUMBER = "[1-9]\\s*(\\w+)";
	List<String> stopwords = null;

	public HashMap<String, List<String>> summaries = new HashMap<String, List<String>>();
	
	public void addSummary(String topicID, String tweet){
		if(summaries.get(topicID) == null){
			summaries.put(topicID, new ArrayList<String>());
		}
		
		summaries.get(topicID).add(tweet);
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
		String path = "/Users/Tutumm/rt_sum/dataset/input/stopwords.txt";
		try {
			stopwords = Files.readAllLines(Paths.get(path));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
	
	public boolean checkDissim(String topicID, String tweet){
		List<String> getSummaries = summaries.get(topicID);
		for(String summary : getSummaries){
			String tweetClean = regEx(tweet);
			String summaryClean = regEx(summary);
			if(compute(cleanText(tweetClean), cleanText(summaryClean)) > 0.5) return false;
		}
		return true;
	}

	public HashMap<String, List<String>> getSummaries() {
		return summaries;
	}

	public void setSummaries(HashMap<String, List<String>> summaries) {
		this.summaries = summaries;
	}
}
