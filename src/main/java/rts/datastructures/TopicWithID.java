package rts.datastructures;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;


public class TopicWithID implements Serializable{

	private static final long serialVersionUID = -7734611102912979011L;
	public HashMap<String, String>  tweetWithIDs = new HashMap<String, String>();

	public TopicWithID() {};
	
	public TopicWithID(String topic2tweetFile) throws IOException {
		
		FileReader file1 = new FileReader(new File(topic2tweetFile));
        BufferedReader read1 = new BufferedReader(file1);
        String line = null;
        String[] word = null;
        while ((line = read1.readLine()) != null) 
        {
        	word = line.split(" ");
            newItem(word[2], word[0]+" "+word[3]);
        }
        read1.close();
	}
	
    public void newItem(String id, String topicIDandLabel){
    	tweetWithIDs.put(id, topicIDandLabel);
    }

    public String getID(String id){
        if (tweetWithIDs.containsKey(id)){
            return tweetWithIDs.get(id);
        }else{
        	return null;
        }
    }

	public HashMap<String, String> getTweetWithIDs() {
		return tweetWithIDs;
	}

	public void setTweetWithIDs(HashMap<String, String> tweetWithIDs) {
		this.tweetWithIDs = tweetWithIDs;
	}
    
    
	
}
