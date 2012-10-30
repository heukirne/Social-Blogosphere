import com.mongodb.*;

import java.util.regex.*;
import java.util.*;
import java.net.*;
import java.io.*;
import java.text.Normalizer;
 
public class RecoTagsDoc {
  
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
		
		int numWords = 40;//Integer.parseInt(args[1]);
		int numTags = 2000;
		
		DBCollection collPosts = mongoDb.getCollection("posts");
		DBCollection collTFIDF = mongoDb.getCollection("tfidf_tag");
		DBCollection collTags = mongoDb.getCollection("tfidf_tagN");
		
		QueryBuilder query = new QueryBuilder();
		BasicDBObject sortDoc = new BasicDBObject();
		
		sortDoc.put("value", -1);
		DBCursor curTags = collTags.find().sort(sortDoc).limit(numTags);
	
		Map<String, Map<String, Double>> allTagsMap = new HashMap<String, Map<String, Double>>();
		while(curTags.hasNext()) {
			DBObject docTag = curTags.next();
			String tag = docTag.get("_id").toString();
			
			DBObject queryDoc = query.start("_id.tag").is(tag).get();
			sortDoc = new BasicDBObject();
			sortDoc.put("tfidf", -1);
			DBCursor curTFIDF = collTFIDF.find(queryDoc).sort(sortDoc).limit(numWords);

			String term = "";
			Double termNorm = 0d, value = 0d;
			Map<String, Double> termMap = new HashMap<String, Double>();
			while(curTFIDF.hasNext()) {
				DBObject obj = curTFIDF.next();
				term = ((BasicDBObject)obj.get("_id")).getString("term");
				if (term.length() > 2) {
					termMap.put(term,((BasicDBObject)obj).getDouble("m"));
				}
			}   	
			allTagsMap.put(tag,termMap);
		
		}
	
		DBCursor curPosts = collPosts.find();//.limit(20);
		BasicDBList list = new BasicDBList();
		while(curPosts.hasNext()) {
			DBObject doc = curPosts.next();
			
			Map<String, Double> docMap = new HashMap<String, Double>();
			String content = doc.get("content").toString();
			String[] parts = Pattern.compile("\\s").split(content);
			for (String part : parts) {
				Double oldCount = docMap.get(part.toLowerCase());
				docMap.put(part, oldCount == null ? 1 : oldCount + 1);
			}
			Double docNorm = 0d;
			for (Double count : docMap.values()) {
			    docNorm += count * count;
			}
			docNorm = Math.sqrt(docNorm);
			
			Double cossine = 0d, termNorm = 0d, value = 0d;
			TreeMap<Double, String> tagMap = new TreeMap<Double, String>();
			for(String tag: allTagsMap.keySet()) {
				Map<String, Double> termTag = allTagsMap.get(tag);
				termNorm = 0d;
				for (Double count : termTag.values()) {
					termNorm += count * count;
				}
				termNorm = Math.sqrt(termNorm);
				cossine = 0d;
				for(String w: termTag.keySet()) {
					value = docMap.get(w);
					if (value!=null) cossine += value*termTag.get(w);
				}
				cossine = cossine / (docNorm * termNorm);

				if (cossine > 0d) tagMap.put(cossine,tag);
			}

			while(tagMap.size()>5) {
				tagMap.remove(tagMap.firstKey());
			}

			list = new BasicDBList();
			String tag = "";
			//System.out.print(doc.get("blogID")+", "+doc.get("postID")+": ");
			for(Double dV: tagMap.keySet()) {
				tag = tagMap.get(dV);
				//System.out.print(tag+", ");
				list.add(list.size(), tag);
			}
			doc.put("listTag", list);
			collPosts.save(doc);
			//System.out.println("!");
			
		}
	
        mongoConn.close();
    }
	
}
