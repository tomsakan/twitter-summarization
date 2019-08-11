package rts.tasks;

import org.apache.flink.api.common.functions.ReduceFunction;
//import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

public class Test implements ReduceFunction<Tuple3<String, JsonNode, Integer>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = -587833445684295371L;

	@Override
	public Tuple3<String, JsonNode, Integer> reduce(Tuple3<String, JsonNode, Integer> arg0, Tuple3<String, JsonNode, Integer> arg1) throws Exception {
		int num1 = arg0.f2;
		int num2 = arg1.f2;
		int sum = num1+num2;
//		System.out.println(arg0.f0+" "+sum+"........");
//		return new Tuple3<String, JsonNode, Integer>(arg0.f0, null, sum);
		return null;
	}

}
