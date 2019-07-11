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
//		if(term.equals("term") && result != 0.0) System.out.println(term +" "+result);
		return result/doc.size();
	}
	
//	idf calculator
	public double idf(Integer size, Integer termCount){
		return Math.log(size / termCount);
	}
	
	public double tfIdf(List<String> doc, Integer size, Integer termCount, String term){
		return tf(doc, term) * idf(size, termCount);
	}
	
}