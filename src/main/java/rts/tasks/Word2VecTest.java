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

	private static double compute(String word1, String word2) {
		WS4JConfiguration.getInstance().setMFS(true);
		double s = new WuPalmer(db).calcRelatednessOfWords(word1, word2);
		return s;
	}
	
	@Override
	public void flatMap(Tuple2<String, JsonNode> node, Collector<Tuple2<String, String>> out) throws Exception {
		if(node.f0.equals("RTS48")){
			
			String text = node.f1.get("original_text").asText() + " " + node.f1.get("original_description").asText();
			
			text = text.replaceAll(URL_REGEX, "");
			text = text.replaceAll("@([^\\s]+)", "");
			text = text.replaceAll("[^\\p{IsDigit}\\p{IsAlphabetic}]", " ");
			text = text.replaceAll("#[A-Za-z]+","");
			text = text.replaceAll("\n", "");
			text = text.replaceAll("RT", "");
			
			ArrayList<String> filteredWords = (ArrayList) Stream.of(text.toLowerCase().split(" ")).collect(Collectors.toCollection(ArrayList<String>::new));
			filteredWords.removeAll(stopwords);
			text = filteredWords.stream().collect(Collectors.joining(" "));
			
			List<String> temp = new ArrayList<>(Arrays.asList(text.split(" ")));
			List<String> terms = new ArrayList<String>();
			
			for(String term : temp){
				if(!(terms.contains(term) || term.equals(" ") || term.equals(""))){
					terms.add(term);
				}
			}
			
			String[] words = new String[terms.size()];
			words = terms.toArray(words);
			
			int count = 0;
			double score = 0.0;
			double max = 0.0;
			for(int i=0; i<words.length-1; i++){
				for(int j=i+1; j<words.length; j++){
					double distance = compute(words[i], words[j]);
					if(distance > max) max = distance;
					System.out.println(words[i] +" -  " +  words[j] + " = " + distance);
				}
				count++;
				score = score + max;
			}
			
			double sim = score/count;
			System.out.println("Label: " + node.f1.get("actual_label").asText() + "\nSimilarity Score: " + sim + "\nTweet: " + node.f1.get("original_text").asText());
			System.out.println("---------------------------------");
//        out.collect(new Tuple2<String, String>(node.f1.get("original_text").asText(), node.f1.get("actual_label").asText()));
		}
	}

}
