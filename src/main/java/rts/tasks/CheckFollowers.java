package rts.tasks;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

public class CheckFollowers implements FilterFunction<Tuple2<String, JsonNode>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = -2308424055763930016L;

	@Override
	public boolean filter(Tuple2<String, JsonNode> node) throws Exception {
		if(node.f1.get("followers_count").asDouble() > 500.0) return true;
		return false;
	}
	

}