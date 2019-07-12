package rts.datastructures;

import java.io.Serializable;
import java.util.HashMap;

public class DocumentsList implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1184660898692967167L;
	public HashMap<String, HashMap<String, String>> documents = new HashMap<String, HashMap<String, String>>();
	
	public DocumentsList() {};
	
	public void addItem(String topicID, String sentence){
//		System.out.print("Hi");
		HashMap<String, String> insideMap = (HashMap<String, String>) documents.get(topicID);
		String[] temp = sentence.split("!@");
		
//		if key is null, create a new map
		if(insideMap == null){
			insideMap = new HashMap<String, String>();
//			temp[0] = tweet id, temp[1] = text
			insideMap.put(temp[0], temp[1]);
			documents.put(topicID, insideMap);
		}else{
//			if(!insideMap.contains(sentence)) termsList.add(sentence);
			insideMap.put(temp[0], temp[1]);
			documents.put(topicID, insideMap);
		}
	}
	
	public void removeTweet(String topicID, String tweetID){
		HashMap<String, String> insideMap = (HashMap<String, String>) documents.get(topicID);
		insideMap.remove(tweetID);
	}
	
	public HashMap<String, String> getDocumentsListByTopic(String topicID){
//		System.out.println(documents);
		if(documents.containsKey(topicID)){
			return documents.get(topicID);
		}else return null;
	}
	
	public HashMap<String, HashMap<String, String>> getDictionary() {
		return documents;
	}

	public void setDictionary(HashMap<String, HashMap<String, String>> dictionary) {
		this.documents = dictionary;
	}
}
