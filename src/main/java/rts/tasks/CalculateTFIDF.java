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
		Collection<String> docs = docsList.getDocumentsListByTopic(node.f0).values();
		
		String[] description = (node.f1.get("description").asText() +" "+node.f1.get("narrative").asText()).split(" ");	
//		Integer df = dfCounter.getDF(node.f0, "term");
		
		for(HashMap.Entry<String, String> entry : docsList.getDocumentsListByTopic(node.f0).entrySet()){
			String key = entry.getKey();
            List<String> doc = Arrays.asList(entry.getValue().split(" "));

            List<String> temp =  Arrays.asList((entry.getValue()+" "+(node.f1.get("description").asText() +" "+node.f1.get("narrative").asText())).split(" "));
            List<String> terms = new ArrayList<String>();
//            creating a term vector for each document combined with description
            for(String term : temp){
                if(!(terms.contains(term) || term.equals(" ") || term.equals(""))) terms.add(term);
            }
            
            TFIDFCalculator calculatorTFIDF = new TFIDFCalculator();
            double[] docArr = new double[terms.size()];
            double[] queryArr = new double[terms.size()];
            int i = 0;
            for(String term : terms){
//            	System.out.println(dfCounter.getDF(node.f0, term));
                double tfidfDoc = calculatorTFIDF.tfIdf(doc, docs.size(), dfCounter.getDF(node.f0, term), term);
                double tfidfQuery = calculatorTFIDF.tfIdf(Arrays.asList(description), docs.size(), dfCounter.getDF(node.f0, term), term);
//                System.out.println(term+"\t"+tfidfDoc+"\t"+tfidfQuery);
//                System.out.println(term);
                docArr[i] = tfidfDoc;
                queryArr[i] = tfidfQuery;
                i++;
//					System.out.println(term + " " + tfidfDoc);
//					System.out.println(term + " " + tfidfQuery);
//					System.out.println(".....");
            }
            
            CosineSimilarityCalculator calculatorCosine = new CosineSimilarityCalculator();
            double cosine = CosineSimilarityCalculator.cosineSimilarity(docArr, queryArr);
            System.out.println(key + " " + cosine);
            System.out.println(".....................");

            
		}
		return null;
	}
}
