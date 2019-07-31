package rts.datastructures;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class DFCounter implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -3567414675202605566L;
	
	public HashMap<String, HashMap<String, Integer>> DFTerms = new HashMap<String, HashMap<String, Integer>>();
	
	public DFCounter() {}
	
	public void addTerms(String topicID, String description, String sentence){

		HashMap<String, Integer> innerMap = (HashMap<String, Integer>) DFTerms.get(topicID);
		List<String> temp = new ArrayList<String>();
//		System.out.println("Hi");
		if(innerMap == null){
			temp =  Arrays.asList(((sentence+" "+description).split(" ")));
		}else{
			temp =  Arrays.asList((sentence.split(" ")));
//			System.out.print("Hi");
		}
		
		List<String> terms = new ArrayList<String>();
		
		for(String term : temp){
			if(!(terms.contains(term) || term.equals(" ") || term.equals(""))){
				terms.add(term);
			}
		}
		
		if(innerMap == null){
			innerMap = new HashMap<String, Integer>();
			for(String term : terms){
				innerMap.put(term, 1);
				DFTerms.put(topicID, innerMap);
			}
		}else{
			for(String term : terms){
				if(innerMap.get(term) == null){
					innerMap.put(term, 1);
				}else{
					innerMap.put(term, innerMap.get(term)+1);
				}
				DFTerms.put(topicID, innerMap);
			}
		}
	}
	
	public Integer getDF(String topicId, String term){
		return DFTerms.get(topicId).get(term);
	}
	
	public HashMap<String, HashMap<String, Integer>> getDFTerms() {
		return DFTerms;
	}

	public void setDFTerms(HashMap<String, HashMap<String, Integer>> dFTerms) {
		DFTerms = dFTerms;
	};
	
	

}
