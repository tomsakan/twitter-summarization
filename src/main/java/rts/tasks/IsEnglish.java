package rts.tasks;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

public class IsEnglish implements FilterFunction<Tuple2<String, JsonNode>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = -4592354039113054382L;

	@Override
	public boolean filter(Tuple2<String, JsonNode> node) throws Exception {
//		if(node.f1.get("actual_label").asInt() < 1){
//			if(node.f1.get("statuses_count").asInt() <= 100){
//				System.out.println(node.f1.get("statuses_count"));
//			}
//		}
		if(node.f1.get("lang").asText().equals("en")) return true;
//			System.out.println(node.f1.get("statuses_count"));
//		else System.out.println(node.f1.get("lang").asText());
		return false;
	}
	

}
