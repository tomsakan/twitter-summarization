package rts.tasks;


import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class ExtractTimeStamp implements AssignerWithPeriodicWatermarks<Tuple2<String, JsonNode>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 584824334019790704L;
	private long currentMaxTimestamp=0;
	
	@Override
	public long extractTimestamp(Tuple2<String, JsonNode> element, long previousElementTimestamp) {

		String time = element.f1.get("created_at").asText();
		SimpleDateFormat sdf = new SimpleDateFormat("EE MMM dd HH:mm:ss zzz yyyy");
		String dateInString = time;
		Date date;
		try {
			date = sdf.parse(dateInString);
			long timestamp = date.getTime();
			currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
			return date.getTime();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

	@Override
	public Watermark getCurrentWatermark() {
		// TODO Auto-generated method stub
		return new Watermark(currentMaxTimestamp);
	}

}
