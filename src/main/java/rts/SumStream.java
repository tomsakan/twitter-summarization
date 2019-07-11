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

import rts.tasks.CheckLength;
import rts.tasks.CombinedDescription;
import rts.tasks.TweetParser;
import rts.tasks.FilterNoLabels;
import rts.tasks.PreProcessing;
import rts.tasks.ContainsKeyWords;



public class SumStream {
	
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
        
//	   	get twitter data from a json file
		DataStreamSource<String> twitterData = env.readTextFile(params.get("inputTest3"));
		DataStream<JsonNode> parsedDataWithTopicID = twitterData.map(new TweetParser("/Users/Tutumm/rt_sum/dataset/input/test1.txt")).filter(new FilterNoLabels());
		    
//		combine the final tweet which contains most improtant features for summarization. e.g. topic id, description, title and assessed label.
		DataStream<Tuple2<String, JsonNode>> finalData =  parsedDataWithTopicID.map(new CombinedDescription("/Users/Tutumm/rt_sum/dataset/input/test2.json"));
		    
//		preprocess the tweets which remove urls, @, hashtags, character repetition, words starting with a number etc. Additionally, create
//		the documents list which contain keys(topics) and the tweets. 
		DataStream<Tuple2<String, JsonNode>> preprocessed = finalData.map(new PreProcessing("/Users/Tutumm/rt_sum/dataset/input/stopwords.txt"))
				.filter(new ContainsKeyWords())
				.filter(new CheckLength());
		
//				create a map of strings which share the same topic id key	
		DataStream<Tuple3<String, String, Double>> output = preprocessed.keyBy(0).map(new CreateDocumentsList())
				.map(new CalculateTFIDF());
//				.map(new Test());
		
//		System.out.println(GlobalVar.dict);
//		addDictionary.writeAsText(params.get("output"));
	    env.execute("Twitter Summarization");
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


