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

import rts.tasks.TweetParser;


public class SumStream {

	
	
	static class TopicWithDescription{
		public HashMap<String, String> topicWithDes = new HashMap<String, String>();
		
		public void newItem(String topid, String description){
			topicWithDes.put(topid, description);
		}
		
		public String getDescription(String topid){
			if(topicWithDes.containsKey(topid)){
				return topicWithDes.get(topid);
			}else return null;
		}
	}
	
	static class DocumentsList{
		public HashMap<String, HashMap<String, String>> dictionary = new HashMap<String, HashMap<String, String>>();
		
		
		public void addItem(String topicID, String sentence){

			HashMap<String, String> insideMap = (HashMap<String, String>) dictionary.get(topicID);
			String[] temp = sentence.split("!@");
			
//			if key is null, create a new map
			if(insideMap == null){
				insideMap = new HashMap<String, String>();
//				temp[0] = tweet id, temp[1] = text
				insideMap.put(temp[0], temp[1]);
				dictionary.put(topicID, insideMap);
			}else{
//				if(!insideMap.contains(sentence)) termsList.add(sentence);
				insideMap.put(temp[0], temp[1]);
				dictionary.put(topicID, insideMap);
			}
		}
		
		public HashMap<String, String> getDocumentsListByTopic(String topicID){
			if(dictionary.containsKey(topicID)){
				return dictionary.get(topicID);
			}else return null;
		}
		
//		public Collection<String> getDocumentsListByTopic(String topicID){
//			if(dictionary.containsKey(topicID)){
//				return dictionary.get(topicID).values();
//			}else return null;
//		}
	}
	
//	static class  TermCollections{
//		public HashMap<String, HashMap<String, Double>> termCollections = new HashMap<String, HashMap<String, Double>>();
//		
//		public void addTerm(String topicID, String term, Double tfidf){
//
//			HashMap<String, Double> insideMap = (HashMap<String, Double>) termCollections.get(topicID);
//			
////			if key is null, create a new map
//			if(insideMap == null){
//				insideMap = new HashMap<String, Double>();
//				insideMap.put(term, tfidf);
//				termCollections.put(topicID, insideMap);
//			}else{
////				if(!insideMap.contains(sentence)) termsList.add(sentence);
//				insideMap.put(temp[0], temp[1]);
//				dictionary.put(topicID, insideMap);
//			}
//		}
//	}
	
	static class CosineSimilarityList{
		public HashMap<String, HashMap<String, Double>> cosineList = new HashMap<String, HashMap<String, Double>>();
		
		
		public void addItem(String topicID, String id, Double cosineSim){

			HashMap<String, Double> insideMap = (HashMap<String, Double>) cosineList.get(topicID);
			
//			if key is null, create a new map
			if(insideMap == null){
				insideMap = new HashMap<String, Double>();
				insideMap.put(id, cosineSim);
				cosineList.put(topicID, insideMap);
			}else{
				insideMap.put(id, cosineSim);
				cosineList.put(topicID, insideMap);
			}
		}
		
		public HashMap<String, Double> getCosineSim(String topicID){
			if(cosineList.containsKey(topicID)){
				return cosineList.get(topicID);
			}else return null;
		}
		
//		public Collection<String> getDocumentsListByTopic(String topicID){
//			if(dictionary.containsKey(topicID)){
//				return dictionary.get(topicID).values();
//			}else return null;
//		}
	}
	
	static class GlobalVar{
		public static TopicWithDescription td = new TopicWithDescription();
		public static List<String> stopwords;
		public static DocumentsList docsList = new DocumentsList();
		public static CosineSimilarityList cosineSim = new CosineSimilarityList();
	}
	
	static class tfIdfCalculator{
//		tf calculator
		static double tf(List<String> doc, String term){
			double result = 0;
			for(String word : doc) {
				if(term.equalsIgnoreCase(word)){
					result++;
				}
			}
//			if(term.equals("term") && result != 0.0) System.out.println(term +" "+result);
			return result/doc.size();
		}
		
//		idf calculator
		static double idf(List<List<String>> docs, String term){
//			System.out.println(docs);
			double n = 0;
			for(List<String> doc : docs){
				for(String word : doc){
					if(term.equals(word)){
						n++;
						break;
					}
				}
			}
//			System.out.println(docs.size() / n);
			return Math.log(docs.size() / n);
		}
		
		static double tfIdf(List<String> doc, List<List<String>> docs, String term){
			return tf(doc, term) * idf(docs, term);
		}
		
	}
	
	static class CosineSimilarityCalculator{
		
		static double cosineSimilarity(double[] queryVector, double[] documentVector){
			double dotProduct = 0.0;
			double normA = 0.0;
			double normB = 0.0;
			
			for(int i = 0; i < queryVector.length; i++){
				dotProduct += queryVector[i] * documentVector[i];
		        normA += Math.pow(queryVector[i], 2);
		        normB += Math.pow(documentVector[i], 2);
			}
//			System.out.println(dotProduct);
			return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
		}
	}

	public static void main(String[] args) throws Exception
	{
//		set the environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		get the parameters
		ParameterTool params = ParameterTool.fromArgs(args);
	    env.getConfig().setGlobalJobParameters(params);
	    

        
//      read file that contains topic description and title
        FileReader file2 = new FileReader(new File("/Users/Tutumm/rt_sum/dataset/input/test2.json"));
        BufferedReader read2 = new BufferedReader(file2);
        String line = null;
        String word = null;
        ObjectMapper jsonParser = new ObjectMapper();
        while ((line = read2.readLine()) != null) 
        {
        	JsonNode node = jsonParser.readValue(line, JsonNode.class);
        	GlobalVar.td.newItem(node.get("topid").asText(), line);
        }
        
//      read all stopwords in the file to a list of strings
        GlobalVar.stopwords = Files.readAllLines(Paths.get("/Users/Tutumm/rt_sum/dataset/input/stopwords.txt"));
        
//	   	get twitter data from a json file
		DataStreamSource<String> twitterData = env.readTextFile(params.get("inputTest3"));
		DataStream<JsonNode> parsedDataWithTopicID = twitterData.map(new TweetParser("/Users/Tutumm/rt_sum/dataset/input/test1.txt")).filter(new FilterNoLabel());
		    
//		combine the final tweet which contains most improtant features for summarization. e.g. topic id, description, title and assessed label.
		DataStream<Tuple2<String, JsonNode>> finalData =  parsedDataWithTopicID.map(new CombinedDescription());
		    
//		preprocess the tweets which remove urls, @, hashtags, character repetition, words starting with a number etc. Additionally, create
//		the documents list which contain keys(topics) and the tweets. 
		DataStream<Tuple2<String, JsonNode>> preprocessed = finalData.map(new PreProcessing())
				.filter(new ContainsKeywords())
				.filter(new CheckLength());
		
//				create a map of strings which share the same topic id key	
		DataStream<Tuple3<String, String, Double>> output = preprocessed.keyBy(0).map(new CreateDocumentsList())
				.map(new CalculateTFIDF());
//				.map(new Test());
		
//		System.out.println(GlobalVar.dict);
//		addDictionary.writeAsText(params.get("output"));
	    env.execute("Twitter Summarization");
	}
	
//	public static class Test implements MapFunction<Tuple2<String, JsonNode>, Tuple2<String, JsonNode>>{
//		public Tuple2<String, JsonNode> map(Tuple2<String, JsonNode> node){
//			if(node.f0.equals("RTS47")) System.out.println(GlobalVar.docsList.getDocumentsListByTopic("RTS47"));
//			return null;
//		}
//	}
	
	
	public static class FilterNoLabel implements FilterFunction<JsonNode>{
		public boolean filter(JsonNode node){
			if(node.get("assessed_label") != null){
				return true;
			}else return false;
		}
	}

	public static class CombinedDescription implements MapFunction<JsonNode, Tuple2<String, JsonNode>>{
		public Tuple2<String, JsonNode> map(JsonNode node) throws Exception{
			
			ObjectMapper jsonParser = new ObjectMapper();
			
			JsonNode finalNode = node;
			
			try{
				String line = GlobalVar.td.getDescription(node.get("topic_id").asText());
				JsonNode description = jsonParser.readValue(line, JsonNode.class);
				((ObjectNode) finalNode).put("narrative", description.get("narrative").asText());
	    		((ObjectNode) finalNode).put("description", description.get("description").asText());
	    		((ObjectNode) finalNode).put("title", description.get("title").asText());
	    		
	    		return new Tuple2<String, JsonNode>(finalNode.get("topic_id").asText(), finalNode);
			}catch(Exception e){
				return null;
			}
		}
	}

	public static class PreProcessing implements MapFunction<Tuple2<String, JsonNode>, Tuple2<String, JsonNode>>{
		
		private final static String URL_REGEX = "((www\\.[\\s]+)|(https?://[^\\s]+))";
		private final static String CONSECUTIVE_CHARS = "([a-z])\\1{1,}";
		private final static String STARTS_WITH_NUMBER = "[1-9]\\s*(\\w+)";
		
		public Tuple2<String, JsonNode> map(Tuple2<String, JsonNode> node) throws Exception{
			String tweet = node.f1.get("full_text").asText();
			String narrative = node.f1.get("narrative").asText();
			String description = node.f1.get("description").asText();
			String title = node.f1.get("title").asText();
			
//			remove urls
			tweet = tweet.replaceAll(URL_REGEX, "");
			narrative = narrative.replaceAll(URL_REGEX, "");
			description = description.replaceAll(URL_REGEX, "");
			title = title.replaceAll(URL_REGEX, "");
			
//			remove username
			tweet = tweet.replaceAll("@([^\\s]+)", "");
			narrative = narrative.replaceAll("@([^\\s]+)", "");
			description = description.replaceAll("@([^\\s]+)", "");
			title = title.replaceAll("@([^\\s]+)", "");
			
//			remove character repetition
//			tweet = tweet.replaceAll(CONSECUTIVE_CHARS, "$1");
//			narrative = narrative.replaceAll(CONSECUTIVE_CHARS, "$1");
//			description = description.replaceAll(CONSECUTIVE_CHARS, "$1");
//			title = title.replaceAll(CONSECUTIVE_CHARS, "$1");
			
//			remove words starting with a number
//			tweet = tweet.replaceAll(STARTS_WITH_NUMBER, "");
//			narrative = narrative.replaceAll(STARTS_WITH_NUMBER, "");
//			description = description.replaceAll(STARTS_WITH_NUMBER, "");
//			title = title.replaceAll(STARTS_WITH_NUMBER, "");
			
//			remove everything that is not alphabet or number
			tweet = tweet.replaceAll("[^\\p{IsDigit}\\p{IsAlphabetic}]", " ");
			narrative = narrative.replaceAll("[^\\p{IsDigit}\\p{IsAlphabetic}]", " ");
			description = description.replaceAll("[^\\p{IsDigit}\\p{IsAlphabetic}]", " ");
			title = title.replaceAll("[^\\p{IsDigit}\\p{IsAlphabetic}]", " ");
			
//			remove hashtags
			tweet = tweet.replaceAll("#[A-Za-z]+","");
			
//			remove \n
			tweet = tweet.replaceAll("\n", " ");
			narrative = narrative.replaceAll("\n", " ");
			description = description.replaceAll("\n", " ");
			title = title.replaceAll("\n", " ");
			
//			remove rt
			tweet = tweet.replaceAll("RT", "");
			
//			remove stopwords
			ArrayList<String> filteredWords = (ArrayList) Stream.of(tweet.toLowerCase().split(" ")).collect(Collectors.toCollection(ArrayList<String>::new));
			filteredWords.removeAll(GlobalVar.stopwords);
//			System.out.println(filteredWords);
			tweet = filteredWords.stream().collect(Collectors.joining(" "));
			
			filteredWords = (ArrayList) Stream.of(narrative.toLowerCase().split(" ")).collect(Collectors.toCollection(ArrayList<String>::new));
			filteredWords.removeAll(GlobalVar.stopwords);
			narrative = filteredWords.stream().collect(Collectors.joining(" "));
			
			filteredWords = (ArrayList) Stream.of(description.toLowerCase().split(" ")).collect(Collectors.toCollection(ArrayList<String>::new));
			filteredWords.removeAll(GlobalVar.stopwords);
			description = filteredWords.stream().collect(Collectors.joining(" "));
			
			filteredWords = (ArrayList) Stream.of(title.toLowerCase().split(" ")).collect(Collectors.toCollection(ArrayList<String>::new));
			filteredWords.removeAll(GlobalVar.stopwords);
			title = filteredWords.stream().collect(Collectors.joining(" "));
			
			ObjectMapper mapper = new ObjectMapper();
    		String json = "{\"id\":\""+ node.f1.get("id").asText() +"\"}";
    		
    		JsonNode preprocessedJson = mapper.readTree(json);
			
    		((ObjectNode) preprocessedJson).put("text",tweet);
    		((ObjectNode) preprocessedJson).put("narrative",narrative);
    		((ObjectNode) preprocessedJson).put("description",description);
    		((ObjectNode) preprocessedJson).put("title",title);
    		((ObjectNode) preprocessedJson).put("original_text",node.f1.get("full_text").asText());
    		((ObjectNode) preprocessedJson).put("actual_label", node.f1.get("assessed_label").asText());
			
			return new Tuple2<String, JsonNode>(node.f0, preprocessedJson);
		}
	}

	public static class ContainsKeywords implements FilterFunction<Tuple2<String, JsonNode>>{
		public boolean filter(Tuple2<String, JsonNode> node){
			ArrayList<String> al= new ArrayList<String>();
			
			String[] description = node.f1.get("description").asText().toLowerCase().split(" ");
			String[] narrative = node.f1.get("narrative").asText().toLowerCase().split(" ");
			String[] title = node.f1.get("title").asText().toLowerCase().split(" ");
			
			for(String word : description) al.add(word);
			for(String word : narrative) al.add(word);
			for(String word : title) al.add(word);
			
			String tweet = node.f1.get("text").asText().toLowerCase();
			
			return al.parallelStream().anyMatch(tweet::contains);
		}
	}

	public static class CheckLength implements FilterFunction<Tuple2<String, JsonNode>>{
		public boolean filter(Tuple2<String, JsonNode> node){
			return node.f1.get("text").asText().split("\\s+").length >= 5;
		}
	}
	
	public static class CreateDocumentsList implements MapFunction<Tuple2<String, JsonNode>, Tuple2<String, JsonNode>>{
		public Tuple2<String, JsonNode> map(Tuple2<String, JsonNode> node){
			GlobalVar.docsList.addItem(node.f0, node.f1.get("id").asText()+"!@"+node.f1.get("text").asText());
			return node;
		}
	}
	
	public static class CalculateTFIDF implements MapFunction<Tuple2<String, JsonNode>, Tuple3<String, String, Double>>{
		public Tuple3<String, String, Double> map(Tuple2<String, JsonNode> node){
			
//			System.out.println(GlobalVar.docsList.getDocumentsListByTopic(node.f0).values());
			Collection<String> docs = GlobalVar.docsList.getDocumentsListByTopic(node.f0).values();
			List<List<String>> DocsList = new ArrayList<List<String>>();
			List<String> innerList = new ArrayList<String>();
			
			String[] description = (node.f1.get("description").asText() +" "+node.f1.get("narrative").asText()).split(" ");	
			
			innerList.addAll(Arrays.asList(description));
			DocsList.add(innerList);
			
			
			for( String doc : docs){
				List<String> a1 = new ArrayList<>();
				 a1.addAll(Arrays.asList(doc.split(" ")));
				DocsList.add(a1);
			}
			
			
//			System.out.println(docs);
			
//			HashMap<String, String> innerMap = GlobalVar.docsList.getDocumentsListByTopic(node.f0);
			for(HashMap.Entry<String, String> entry : GlobalVar.docsList.getDocumentsListByTopic(node.f0).entrySet()){
				String key = entry.getKey();
				List<String> doc = Arrays.asList(entry.getValue().split(" "));
				System.out.println(node.f0 + key + doc);
				
				List<String> temp =  Arrays.asList((entry.getValue()+" "+(node.f1.get("description").asText() +" "+node.f1.get("narrative").asText())).split(" "));
				
				List<String> terms = new ArrayList<String>();
				
				for(String term : temp){
					if(!(terms.contains(term) || term.equals(" ") || term.equals(""))) terms.add(term);
				}
				
				tfIdfCalculator calculatorTFIDF = new tfIdfCalculator();


				double[] docArr = new double[terms.size()];
				double[] queryArr = new double[terms.size()];
				int i = 0;
				for(String term : terms){
					double tfidfDoc = calculatorTFIDF.tfIdf(doc, DocsList, term);
					double tfidfQuery = calculatorTFIDF.tfIdf(Arrays.asList(description), DocsList, term);
					docArr[i] = tfidfDoc;
					queryArr[i] = tfidfQuery;
					i++;
//					System.out.println(term + " " + tfidfDoc);
//					System.out.println(term + " " + tfidfQuery);
//					System.out.println(".....");
				}
				
				CosineSimilarityCalculator calculatorCosine = new CosineSimilarityCalculator();
				double cosine = CosineSimilarityCalculator.cosineSimilarity(docArr, queryArr);
//				System.out.println(key + " " + cosine);
//				System.out.println(".....................");
//				return new Tuple3<String, String, Double>(node.f0, key, cosine);
			}
			return null;
		}
	}
}


