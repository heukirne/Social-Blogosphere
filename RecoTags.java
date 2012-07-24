import com.mongodb.*;

import java.util.regex.*;
import java.util.*;
import java.net.*;
import java.io.*;
import java.text.Normalizer;
 
public class RecoTags {
  
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
		
		String tag = "saude";

		/* Get list of Tag-frequency-words */
		DBCollection collTFIDF = mongoDb.getCollection("tfidf_title");

		QueryBuilder query = new QueryBuilder();
		DBObject queryDoc = query.start("_id.tag").is(tag).and("d").lessThan(4000).get();

		BasicDBObject sortDoc = new BasicDBObject();
        sortDoc.put("R", -1);

        DBCursor cur = collTFIDF.find(queryDoc).sort(sortDoc).limit(800);

        String term = "";
		Map<String, Double> termMap = new HashMap<String, Double>();
		while(cur.hasNext()) {
			DBObject obj = cur.next();
			term = ((BasicDBObject)obj.get("_id")).getString("term");  
			termMap.put(term,((BasicDBObject)obj).getDouble("R"));
			//System.out.println(term); 
		}   
		

		/*Get Title words*/
		DBCollection collPosts = mongoDb.getCollection("posts");
		DBObject queryPosts = query.start("tags").is(tag).get();
		DBCursor curPosts = collPosts.find(queryPosts);
		String title = "";
		String[] parts;
		Double value,cont;
		while(curPosts.hasNext()) {
			cont = 0d;
			DBObject obj = curPosts.next();
			title = obj.get("title").toString();
			parts = Pattern.compile("\\s").split(title.replaceAll("[\\W\\d]"," ").replaceAll("\\s+"," "));
			for (String part : parts) {
				value = termMap.get(part.toLowerCase());
				if (value!=null) cont += value;
			}
			obj.put(tag,cont);
			collPosts.save(obj);
			//System.out.println(cont);
		}

		DBObject queryAccur = query.start("tags").is(tag).and(tag).greaterThan(0).get();
		//System.out.println("Total:"+collPosts.getCount(queryPosts));
		System.out.println("Accur:"+collPosts.getCount(queryAccur));


        mongoConn.close();
    }
	
}
