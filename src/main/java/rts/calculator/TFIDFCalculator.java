package rts.calculator;

import java.io.Serializable;
import java.util.List;

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
	public double idf(List<List<String>> docs, String term){
//		System.out.println(docs);
		double n = 0;
		for(List<String> doc : docs){
			for(String word : doc){
				if(term.equals(word)){
					n++;
					break;
				}
			}
		}
//		System.out.println(docs.size() / n);
		return Math.log(docs.size() / n);
	}
	
	public double tfIdf(List<String> doc, List<List<String>> docs, String term){
		return tf(doc, term) * idf(docs, term);
	}
	
}