import com.mongodb.*;

import java.util.regex.*;
import java.util.*;
import java.net.*;
import java.io.*;
import java.text.Normalizer;
 
public class AddTags {
  
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
		int numWords = 100;

		/* Get list of Tag-frequency-words */
		DBCollection collTFIDF = mongoDb.getCollection("tfidf_tag5");
		DBCollection collTags = mongoDb.getCollection("allTags");

		QueryBuilder query = new QueryBuilder();

		BasicDBObject sortTags = new BasicDBObject();
        sortTags.put("value", -1);	
		DBCursor curTags = collTags.find().sort(sortTags).limit(10);
		while(curTags.hasNext()) {
			DBObject obj = curTags.next();
			System.out.println(obj.get("_id")); 
		}   


		System.exit(1);


		DBObject queryTF = query.start("_id.tag").is(tag).and("m").lessThan(13000).get();
		BasicDBObject sortTF = new BasicDBObject();
        sortTF.put("tfidf", -1);

        DBCursor curTF = collTFIDF.find(queryTF).sort(sortTF).limit(numWords);

        String term = "";

Collection<Map<String,Double>> termMap = new ArrayList<Map<String,Double>>();
termMap.add(new HashMap<String,Double>());

		//Map<String, Double>[] termMap = new HashMap<String, Double>()[10];
		while(curTF.hasNext()) {
			DBObject obj = curTF.next();
			term = ((BasicDBObject)obj.get("_id")).getString("term");  
			termMap.get(0).put(term,((BasicDBObject)obj).getDouble("tfidf"));
			//System.out.println(term); 
		}   
		

		/*Get Title words*/
		DBCollection collPosts = mongoDb.getCollection("posts");
		DBObject queryPosts = query.start("tags").size(5).get();
		DBCursor curPosts = collPosts.find(queryPosts);
		BasicDBList list = new BasicDBList();
		String title = "";
		String[] parts;
		Double value,cont;
		while(curPosts.hasNext()) {
			cont = 0d;
			DBObject obj = curPosts.next();
			title = obj.get("content").toString();
			parts = Pattern.compile("\\s").split(title.replaceAll("[\\W\\d]"," ").replaceAll("\\s+"," "));
			for (String part : parts) {
				value = termMap[0].get(part.toLowerCase());
				if (value!=null) cont += value;
			}


			if (cont > 0.01d) {

				list = new BasicDBList();
				if (obj.containsField("listTag"))
					list = (BasicDBList)obj.get("listTag");

				list.add(list.size(), tag);
				obj.put("listTag", list); 
				collPosts.save(obj);
			}



			//System.out.println(cont);
		}

		DBObject queryAccur = query.start("listTag").is(tag).and("tags").size(5).get();
		System.out.println(numWords+" palavras em "+tag);
		System.out.println("Total:"+collPosts.getCount(queryPosts));
		System.out.println("Accur:"+collPosts.getCount(queryAccur));


        mongoConn.close();
    }
	
}
