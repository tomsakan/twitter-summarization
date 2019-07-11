package rts.tasks;

import java.util.Collection;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import rts.datastructures.DocumentsList;


public class CreateDocumentList implements MapFunction<Tuple2<String, JsonNode>, Tuple2<String, JsonNode>>{
	/**
	 * 
	 */
	private static final long serialVersionUID = -1067702738214494016L;
	
	DocumentsList docsList = new DocumentsList();

	public Tuple2<String, JsonNode> map(Tuple2<String, JsonNode> node){
//		System.out.println(node);
		docsList.addItem(node.f0, node.f1.get("id").asText()+"!@"+node.f1.get("text").asText());
		Collection<String> docs = docsList.getDocumentsListByTopic(node.f0).values();
		System.out.println(docs);
		return node;
	}
}