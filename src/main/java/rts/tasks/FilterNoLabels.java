package rts.tasks;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

public class FilterNoLabels implements FilterFunction<JsonNode>{

	private static final long serialVersionUID = -8780818979576721406L;

	public boolean filter(JsonNode node){
		if(node.get("assessed_label") != null){
			return true;
		}else return false;
	}
}
