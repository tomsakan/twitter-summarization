package rts.tasks;

import java.io.Serializable;

import org.tartarus.snowball.ext.PorterStemmer;

public class Stemmer implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -6278776986099032333L;
	
	public Stemmer() {};
	
	public String stemText(String context) {
		PorterStemmer stemmer = new PorterStemmer();
		String[] temp = context.split(" ");
		String out = "";
		
		for(String word : temp){
			stemmer.setCurrent(word);
			stemmer.stem();
			out += stemmer.getCurrent()+" ";
		}
		return out;
		
	}

}
