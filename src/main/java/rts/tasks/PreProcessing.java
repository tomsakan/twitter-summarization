package rts.tasks;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public  class PreProcessing implements MapFunction<Tuple2<String, JsonNode>, Tuple2<String, JsonNode>>{
	
	private static final long serialVersionUID = 1959520369063222574L;
	
	private final static String URL_REGEX = "((www\\.[\\s]+)|(https?://[^\\s]+))";
	private final static String CONSECUTIVE_CHARS = "([a-z])\\1{1,}";
	private final static String STARTS_WITH_NUMBER = "[1-9]\\s*(\\w+)";
	List<String> stopwords = null;
	
	public PreProcessing(String path) throws IOException{
		stopwords = Files.readAllLines(Paths.get(path));
	}
	
	public Tuple2<String, JsonNode> map(Tuple2<String, JsonNode> node) throws Exception{
		String tweet = node.f1.get("full_text").asText();
		String narrative = node.f1.get("narrative").asText();
		String description = node.f1.get("description").asText();
		String title = node.f1.get("title").asText();
		
//		remove urls
		tweet = tweet.replaceAll(URL_REGEX, "");
		narrative = narrative.replaceAll(URL_REGEX, "");
		description = description.replaceAll(URL_REGEX, "");
		title = title.replaceAll(URL_REGEX, "");
		
//		remove username
		tweet = tweet.replaceAll("@([^\\s]+)", "");
		narrative = narrative.replaceAll("@([^\\s]+)", "");
		description = description.replaceAll("@([^\\s]+)", "");
		title = title.replaceAll("@([^\\s]+)", "");
		
//		remove everything that is not alphabet or number
		tweet = tweet.replaceAll("[^\\p{IsDigit}\\p{IsAlphabetic}]", " ");
		narrative = narrative.replaceAll("[^\\p{IsDigit}\\p{IsAlphabetic}]", " ");
		description = description.replaceAll("[^\\p{IsDigit}\\p{IsAlphabetic}]", " ");
		title = title.replaceAll("[^\\p{IsDigit}\\p{IsAlphabetic}]", " ");
		
//		remove hashtags
		tweet = tweet.replaceAll("#[A-Za-z]+","");
		
//		remove \n
		tweet = tweet.replaceAll("\n", " ");
		narrative = narrative.replaceAll("\n", " ");
		description = description.replaceAll("\n", " ");
		title = title.replaceAll("\n", " ");
		
//		remove rt
		tweet = tweet.replaceAll("RT", "");
		
//		remove stopwords
		ArrayList<String> filteredWords = (ArrayList) Stream.of(tweet.toLowerCase().split(" ")).collect(Collectors.toCollection(ArrayList<String>::new));
		filteredWords.removeAll(stopwords);
//		System.out.println(filteredWords);
		tweet = filteredWords.stream().collect(Collectors.joining(" "));
		
		filteredWords = (ArrayList) Stream.of(narrative.toLowerCase().split(" ")).collect(Collectors.toCollection(ArrayList<String>::new));
		filteredWords.removeAll(stopwords);
		narrative = filteredWords.stream().collect(Collectors.joining(" "));
		
		filteredWords = (ArrayList) Stream.of(description.toLowerCase().split(" ")).collect(Collectors.toCollection(ArrayList<String>::new));
		filteredWords.removeAll(stopwords);
		description = filteredWords.stream().collect(Collectors.joining(" "));
		
		filteredWords = (ArrayList) Stream.of(title.toLowerCase().split(" ")).collect(Collectors.toCollection(ArrayList<String>::new));
		filteredWords.removeAll(stopwords);
		title = filteredWords.stream().collect(Collectors.joining(" "));
		
//		Stemming
		Stemmer stemmer = new Stemmer();
		tweet = stemmer.stemText(tweet);
		narrative = stemmer.stemText(narrative);
		description = stemmer.stemText(description);
		title = stemmer.stemText(title);
		
		ObjectMapper mapper = new ObjectMapper();
		String json = "{\"id\":\""+ node.f1.get("id").asText() +"\"}";
		
		JsonNode preprocessedJson = mapper.readTree(json);
		
		((ObjectNode) preprocessedJson).put("text",tweet);
		((ObjectNode) preprocessedJson).put("narrative",narrative);
		((ObjectNode) preprocessedJson).put("description",description);
		((ObjectNode) preprocessedJson).put("title",title);
		((ObjectNode) preprocessedJson).put("original_text",node.f1.get("full_text").asText());
		((ObjectNode) preprocessedJson).put("actual_label", node.f1.get("assessed_label").asText());
		((ObjectNode) preprocessedJson).put("lang", node.f1.get("lang").asText());
		((ObjectNode) preprocessedJson).put("statuses_count", node.f1.get("user").get("statuses_count").asInt());
		((ObjectNode) preprocessedJson).put("verified", node.f1.get("user").get("verified").asText());
		
		return new Tuple2<String, JsonNode>(node.f0, preprocessedJson);
	}
}
