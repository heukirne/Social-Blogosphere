import com.mongodb.*;

import java.util.regex.*;
import java.util.*;
import java.net.*;
import java.io.*;
import java.text.Normalizer;

import org.apache.lucene.analysis.br.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.analysis.snowball.*;
import org.apache.lucene.util.*;
 
public class BrStemmer {
  
	private static Mongo mongoConn;
	private static DB mongoDb;
	
    public static void main(String[] args) throws Exception {		

		mongoConn = new Mongo( "localhost" , 27017 );
		mongoDb = mongoConn.getDB( "blogdb" );
		
		try {
			mongoDb.getCollectionNames();
		} catch (Exception e) {
			System.out.println("MongoDB Offline.");
			System.exit(1);
		}
		
		QueryBuilder query = new QueryBuilder();
        DBCollection collPosts = mongoDb.getCollection("posts");
		DBObject queryPosts = query.start("tags").size(5).get();//.and(tag).exists(true).get();
		DBCursor curPosts = collPosts.find(queryPosts);
		String content;
		while(curPosts.hasNext()) {
			DBObject doc = curPosts.next();
			content = doc.get("content").toString();
			content = BrStemmer.Stem(content);
			//System.out.println(content);

			doc.put("content", content); 
			collPosts.save(doc);			
		}

		mongoConn.close();

    }
	
    public static String Stem(String text){
        StringBuffer result = new StringBuffer();
        if (text!=null && text.trim().length()>0){
            StringReader tReader = new StringReader(text);
            Analyzer analyzer = new SnowballAnalyzer(Version.LUCENE_36,"Portuguese");
            analyzer = new BrazilianAnalyzer(Version.LUCENE_36);
            TokenStream tStream = analyzer.tokenStream("contents", tReader);
            TermAttribute term = tStream.addAttribute(TermAttribute.class);

            try {
                while (tStream.incrementToken()){
                    result.append(term.term());
                    result.append(" ");
                }
            } catch (IOException ioe){
                System.out.println("Error: "+ioe.getMessage());
            }
        }

        // If, for some reason, the stemming did not happen, return the original text
        if (result.length()==0)
            result.append(text);
        return result.toString().trim();
    }

}
