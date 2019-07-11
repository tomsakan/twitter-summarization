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
import rts.datastructures.DocumentsList;

public class CalculateTFIDF implements MapFunction<Tuple2<String, JsonNode>, Tuple2<String, JsonNode>>{
	/**
	 * 
	 */
	private static final long serialVersionUID = 3024721444405991043L;
	
	DocumentsList docsList = new DocumentsList();

	public Tuple2<String, JsonNode> map(Tuple2<String, JsonNode> node){
//		System.out.println(node);
//		System.out.println(docsList.getDocumentsListByTopic(node.f0).values());
		docsList.addItem(node.f0, node.f1.get("id").asText()+"!@"+node.f1.get("text").asText());
		Collection<String> docs = docsList.getDocumentsListByTopic(node.f0).values();
		System.out.println(docs);
		return null;
	}
}
