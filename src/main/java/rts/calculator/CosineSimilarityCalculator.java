package rts.calculator;

import java.io.Serializable;

public class CosineSimilarityCalculator implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1392439192187814356L;

	public static double cosineSimilarity(double[] queryVector, double[] documentVector){
		double dotProduct = 0.0;
		double normA = 0.0;
		double normB = 0.0;
		
		for(int i = 0; i < queryVector.length; i++){
			dotProduct += queryVector[i] * documentVector[i];
	        normA += Math.pow(queryVector[i], 2);
	        normB += Math.pow(documentVector[i], 2);
		}
		
		if((Math.sqrt(normA) * Math.sqrt(normB)) == 0.0){
			return dotProduct / 0.1;
		}
//		System.out.println(dotProduct + " "+ (Math.sqrt(normA) * Math.sqrt(normB)));
		return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
	}
}
