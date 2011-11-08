import com.mongodb.*;

import java.util.regex.*;
import java.util.*;
import java.net.*;
import java.io.*;
import java.text.Normalizer;
 
public class StopWords {
  
	private static Mongo mongoConn;
	private static DB mongoDb;
	private static DBCollection collSWords;
	
    public static void main(String[] args) throws Exception {		

    	
		mongoConn = new Mongo( "localhost" , 27017 );
		mongoDb = mongoConn.getDB( "blogdb" );
		
		try {
			mongoDb.getCollectionNames();
		} catch (Exception e) {
			System.out.println("MongoDB Offline.");
			System.exit(1);
		}
		
		collSWords = mongoDb.getCollection("stopWords");	
		collSWords.ensureIndex("word");	

		File file = new File("stopwordsBR2.txt");
		BufferedReader reader = null;
		
		try {
			String text = null;
			reader = new BufferedReader(new FileReader(file));
			 
			while ((text = reader.readLine()) != null) {
				text = Normalizer.normalize(text, Normalizer.Form.NFD);
				text = text.replaceAll("[^\\p{ASCII}]", "");				
				//BasicDBObject doc = new BasicDBObject();
				//doc.put("word", text);
				//collSWords.insert(doc);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (reader != null) {
				reader.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

        mongoConn.close();
    }
	
}