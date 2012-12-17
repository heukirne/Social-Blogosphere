import com.mongodb.*;

import org.apache.lucene.analysis.br.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.analysis.snowball.*;
import org.apache.lucene.util.*;

import java.util.*;
import java.net.*;
import java.io.*;

public class StemmTags {

	public static Analyzer analyzer;

	public static void main(String[] args) throws Exception {

		Mongo mongoConn = new Mongo( "localhost" , 27018 );
		DB mongoDb = mongoConn.getDB( "blogdb" );

		try {
			mongoDb.getCollectionNames();
		} catch (Exception e) {
			System.out.println("MongoDB Offline.");
			System.exit(1);
		}

		DBCollection collPosts = mongoDb.getCollection("posts");
		Analyzer analyzer = new BrazilianAnalyzer(Version.LUCENE_36);
	
		QueryBuilder query = new QueryBuilder();
		DBObject queryPosts = query.start("blogID").is("7088898706087406787").get();
		DBCursor curPosts = collPosts.find();

		int cont=0;
		long total=collPosts.getCount();
		while(curPosts.hasNext()) {
			DBObject doc = curPosts.next();
			if ((cont++)%1000==0) System.out.println(total+" - "+cont);
			BasicDBList list = new BasicDBList();
			BasicDBList newTags = new BasicDBList();
			list = (BasicDBList)doc.get("tags");
			for (Object tag : list) {
				newTags.add(newTags.size(), Stem(tag.toString(),analyzer));
			}
			doc.put("tags", newTags); 
			collPosts.save(doc);
		}

		mongoConn.close();

	}

public static String Stem(String text, Analyzer analyzer){
        StringBuffer result = new StringBuffer();
        if (text!=null && text.trim().length()>0){

            text = text.replaceAll("[\\W\\d_]"," ").replaceAll("\\s+"," "); //need to remove underline "no"
            StringReader tReader = new StringReader(text);
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
