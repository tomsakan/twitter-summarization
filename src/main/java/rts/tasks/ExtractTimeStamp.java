package rts.tasks;


import java.sql.Timestamp;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

public class ExtractTimeStamp extends AscendingTimestampExtractor<Tuple2<String, JsonNode>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = -4945470768641723602L;
	private final SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
	public long extractAscendingTimestamp(Tuple2<String, JsonNode> element)
    {
    	String[] time = element.f1.get("created_at").asText().split(" ");
		String month = "";
		if(time[1].equals("Jan")) month = "01";
		else if(time[1].equals("Feb")) month = "02";
		else if(time[1].equals("Mar")) month = "03";
		else if(time[1].equals("Apr")) month = "04";
		else if(time[1].equals("May")) month = "05";
		else if(time[1].equals("Jun")) month = "06";
		else if(time[1].equals("Jul")) month = "07";
		else if(time[1].equals("Aug")) month = "08";
		else if(time[1].equals("Sep")) month = "09";
		else if(time[1].equals("Oct")) month = "10";
		else if(time[1].equals("Nov")) month = "11";
		else if(time[1].equals("Dec")) month = "12";
		String dateInString = time[2]+"/"+month+"/"+time[5]+" "+time[3];
		try {
			
			Timestamp ts = new Timestamp(sdf.parse(dateInString).getTime());
//			System.out.println(ts.getTime());
			return ts.getTime();
		} catch (ParseException e) {
			throw new java.lang.RuntimeException("Parsing Error");
		}
	}
}