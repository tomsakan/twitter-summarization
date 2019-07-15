package rts.tasks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.tartarus.snowball.ext.PorterStemmer;

import rts.calculator.CosineSimilarityCalculator;
import rts.calculator.TFIDFCalculator;
import rts.datastructures.DFCounter;
import rts.datastructures.DocumentsList;

public class CalculateTFIDF implements MapFunction<Tuple2<String, JsonNode>, Tuple2<String, JsonNode>>{
	/**
	 * 
	 */
	private static final long serialVersionUID = 3024721444405991043L;
	
	DocumentsList docsList = new DocumentsList();
	DFCounter dfCounter = new DFCounter();

	public Tuple2<String, JsonNode> map(Tuple2<String, JsonNode> node){
		docsList.addItem(node.f0, node.f1.get("id").asText()+"!@"+node.f1.get("text").asText());
		dfCounter.addTerms(node.f0, node.f1.get("description").asText() +" "+node.f1.get("narrative").asText(), node.f1.get("text").asText());
//		dfCounter.addTerms(node.f0, node.f1.get("description").asText(), node.f1.get("text").asText());
		Collection<String> docs = docsList.getDocumentsListByTopic(node.f0).values();
		
		String[] description = (node.f1.get("description").asText() +" "+node.f1.get("narrative").asText()).split(" ");	
//		String[] description = (node.f1.get("description").asText()).split(" ");	
		Double cosineForTweet = 0.0;

		for(HashMap.Entry<String, String> entry : docsList.getDocumentsListByTopic(node.f0).entrySet()){
			String key = entry.getKey();
            if(key.equals(node.f1.get("id").asText())){
            	List<String> doc = Arrays.asList(entry.getValue().split(" "));

                List<String> temp =  Arrays.asList((entry.getValue()+" "+(node.f1.get("description").asText() +" "+node.f1.get("narrative").asText())).split(" "));
//            	List<String> temp =  Arrays.asList((entry.getValue()+" "+(node.f1.get("description").asText())).split(" "));
                List<String> terms = new ArrayList<String>();
            	
                for(String term : temp){
                    if(!(terms.contains(term) || term.equals(" ") || term.equals(""))) terms.add(term);
                }
                
                TFIDFCalculator calculatorTFIDF = new TFIDFCalculator();
                double[] docArr = new double[terms.size()];
                double[] queryArr = new double[terms.size()];
                int i = 0;
                
                for(String term : terms){
                  double tfidfDoc = calculatorTFIDF.tfIdf(doc, docs.size(), dfCounter.getDF(node.f0, term), term);
                  double tfidfQuery = calculatorTFIDF.tfIdf(Arrays.asList(description), docs.size(), dfCounter.getDF(node.f0, term), term);
//                  if(key.equals("891817763542630404")) System.out.println(term+"\tdoc: "+tfidfDoc+"\t"+"query: "+tfidfQuery);
                  docArr[i] = tfidfDoc;
                  queryArr[i] = tfidfQuery;
                  i++;
                }
                double cosine = CosineSimilarityCalculator.cosineSimilarity(docArr, queryArr);
                cosineForTweet = cosine;
//                if(key.equals("891817763542630404")) System.out.println(cosineForTweet);
            }else continue;
		}
		
		JsonNode parsedJson = node.f1;
		((ObjectNode) parsedJson).put("cosine_score", cosineForTweet);
//		if(node.f1.get("id").asText().equals("891735060289900545")) System.out.println(cosineForTweet);
//		if(node.f0.equals("RTS48"))System.out.println(parsedJson);
//		if(node.f0.equals("RTS47"))System.out.println(parsedJson.get("id").asText()+"\t\t"+parsedJson.get("actual_label").asText()+"\t\t"+parsedJson.get("cosine_score").asDouble());
		return new Tuple2<String, JsonNode>(node.f0, parsedJson);
	}
}
