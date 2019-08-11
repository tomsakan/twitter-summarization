package rts.tasks;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

public class Test1 implements MapFunction<Tuple2<String, JsonNode>, Tuple3<String, JsonNode, Integer>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = -8755218125801087859L;

	@Override
	public Tuple3<String, JsonNode, Integer> map(Tuple2<String, JsonNode> arg0) throws Exception {
		// TODO Auto-generated method stub
		return new Tuple3<String, JsonNode, Integer>(arg0.f0, arg0.f1, arg0.f1.get("count").asInt());
	}

}
