package rts.datastructures;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import rts.calculator.CosineSimilarityCalculator;
import rts.calculator.TFIDFCalculator;

public class DissimilarityCheck implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 720145350964418763L;
	
	public HashMap<String, List<String>> summaries = new HashMap<String, List<String>>();
	
	public void addSummary(String topicID, String tweet){
		if(summaries.get(topicID) == null){
			summaries.put(topicID, new ArrayList<String>());
		}
		
		summaries.get(topicID).add(tweet);
	}
	
	public boolean checkDissim(String topicID, String tweet){
		List<String> getSummaries = summaries.get(topicID);
		TFIDFCalculator tfidf = new TFIDFCalculator();
		for(String summary : getSummaries){
			List<String> temp =  Arrays.asList((summary+" "+tweet).split(" "));
            List<String> terms = new ArrayList<String>();
            for(String term : temp){
                if(!(terms.contains(term) || term.equals(" ") || term.equals(""))) terms.add(term);
            }
            
            DFCounter counter = new DFCounter();
            
            counter.addTerms(topicID, summary, tweet);
            
            double[] sumArr = new double[terms.size()];
            double[] tweetArr = new double[terms.size()];
            int i = 0;
            
            for(String term : terms){
              double tfidfSum = tfidf.tfIdf(Arrays.asList(summary.split(" ")), 2, counter.getDF(topicID, term), term);
              double tfidfTweet = tfidf.tfIdf(Arrays.asList(tweet.split(" ")), 2, counter.getDF(topicID, term), term);
              sumArr[i] = tfidfSum;
              tweetArr[i] = tfidfTweet;
              i++;
            }
            
            double cosine = CosineSimilarityCalculator.cosineSimilarity(sumArr, tweetArr);
            if(cosine >= 0.5) {
//            	System.out.println(summary + "\n" + tweet + "\n" + cosine +"\n-----------");
            	return false;
            }
//            System.out.println(summary + "\n" + tweet + "\n" + cosine +"\n-----------");
		}
		return true;
	}

	public HashMap<String, List<String>> getSummaries() {
		return summaries;
	}

	public void setSummaries(HashMap<String, List<String>> summaries) {
		this.summaries = summaries;
	}

}
