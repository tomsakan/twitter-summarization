package rts.calculator;

import java.io.Serializable;
import java.util.List;

import rts.datastructures.DFCounter;

public class TFIDFCalculator implements Serializable{
/**
	 * 
	 */
	private static final long serialVersionUID = 1305288307889470182L;

//	tf calculator
	public double tf(List<String> doc, String term){
		double result = 0;
		for(String word : doc) {
			if(term.equalsIgnoreCase(word)){
				result++;
			}
		}
//		if(term.equals("panera")) System.out.println("tf "+term +" "+result);
		return result/(double)(doc.size());
	}
	
//	idf calculator
	public double idf(Integer size, Integer termCount, String term){
//		System.out.println("idf: "+(size / (double)termCount)+"\tsize: "+size +"\tcount: "+termCount);
		return Math.log(1+(size / (double)termCount));
	}
	
	public double tfIdf(List<String> doc, Integer size, Integer termCount, String term){
		return tf(doc, term) * idf(size, termCount, term);
	}
	
}