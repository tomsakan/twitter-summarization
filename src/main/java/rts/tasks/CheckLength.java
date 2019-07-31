package rts.tasks;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

public class CheckLength implements FilterFunction<Tuple2<String, JsonNode>>{

	private static final long serialVersionUID = -3321781097495323209L;

	public boolean filter(Tuple2<String, JsonNode> node){
//		if(node.f1.get("actual_label").asInt() >= 1){
//			if(node.f1.get("text").asText().split("\\s+").length < 5)System.out.println(node.f1.get("text").asText().split("\\s+").length);
//		}
		return node.f1.get("text").asText().split("\\s+").length >= 5;
	}
}