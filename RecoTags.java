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

		if (args.length!=2) {
		   System.out.println("Falta nome da tag ou numero de palavras");
		   System.exit(1);
		}

		mongoConn = new Mongo( "localhost" , 27017 );
		mongoDb = mongoConn.getDB( "blogdb" );
		
		try {
			mongoDb.getCollectionNames();
		} catch (Exception e) {
			System.out.println("MongoDB Offline.");
			System.exit(1);
		}
		
		String tag = args[0];
		int numWords = Integer.parseInt(args[1]);
		int numDoc = 20;

		/* Get list of Tag-frequency-words */
		DBCollection collTFIDF = mongoDb.getCollection("tfidf_tag5");

		QueryBuilder query = new QueryBuilder();
		DBObject queryDoc = query.start("_id.tag").is(tag).and("m").lessThan(15000).get();

		BasicDBObject sortDoc = new BasicDBObject();
        sortDoc.put("tfidf", -1);

        DBCursor cur = collTFIDF.find(queryDoc).sort(sortDoc).limit(numWords);

        String term = "";
        Double termNorm = 0d;
		Map<String, Double> termMap = new HashMap<String, Double>();
		while(cur.hasNext()) {
			DBObject obj = cur.next();
			term = ((BasicDBObject)obj.get("_id")).getString("term");
			if (term.length() > 2) {
				termMap.put(term,((BasicDBObject)obj).getDouble("m"));
				termNorm += ((BasicDBObject)obj).getDouble("m") * ((BasicDBObject)obj).getDouble("m");
			}
			//System.out.println(term); 
		}   
		termNorm = Math.sqrt(termNorm);
		

		/*Get Title words*/
		DBCollection collPosts = mongoDb.getCollection("posts");
		DBObject queryPosts = query.start("tags").is(tag).get();//.and("tagNum").is(5).get();//and(tag).exists(true).get();
		DBCursor curPosts = collPosts.find(queryPosts);//.limit(10);

		DBCollection collDocWords = mongoDb.getCollection("tfidf_word5");
		DBObject queryDocWords;

		BasicDBList list = new BasicDBList();
		String content = "";
		String[] parts;
		Double value,cont,tfWord,total,docNorm;
		Long totalL;
		Integer prod;
		totalL = collPosts.getCount(queryPosts);
		total = totalL.doubleValue();
		docNorm=0d;
		while(curPosts.hasNext()) {
			cont = 0d;
			DBObject doc = curPosts.next();

			/* Compara Doc TFIDF with Tag TFIDF/
			queryDocWords = query.start("_id.id").is(doc.get("postID").toString()).get();
			DBCursor curWords = collDocWords.find(queryDocWords).sort(sortDoc);//.limit(numDoc);
			while(curWords.hasNext()) {
				DBObject word = curWords.next();
				term = ((BasicDBObject)word.get("_id")).getString("term"); 
				tfWord = ((Double)word.get("m")); 
				docNorm += tfWord*tfWord;
				value = termMap.get(term);
				if (value!=null) cont += value*tfWord;
			}
			docNorm = Math.sqrt(docNorm);
			cont = cont / (docNorm * termNorm);
			/**/

			Map<String, Double> docMap = new HashMap<String, Double>();
			cont =0d;
			content = doc.get("content").toString();
			parts = Pattern.compile("\\s").split(content.replaceAll("[\\W\\d]"," ").replaceAll("\\s+"," "));
			for (String part : parts) {
				Double oldCount = docMap.get(part.toLowerCase());
				docMap.put(part, oldCount == null ? 1 : oldCount + 1);

				//value = termMap.get(part.toLowerCase());
				//if (value!=null) cont += value;
			}

			
			docNorm = 0d;
			for (Double count : docMap.values()) {
			    docNorm += count * count;
			}
			docNorm = Math.sqrt(docNorm);
			for(String w: termMap.keySet()) {
				value = docMap.get(w);
				if (value!=null) cont += value*termMap.get(w);
			}
			cont = cont / (docNorm * termNorm);
			

			/* Add Similar Tag to list /
			if (cont > 0d) {

				list = new BasicDBList();
				if (doc.containsField("listTag"))
					list = (BasicDBList)doc.get("listTag");

				list.add(list.size(), tag);
				doc.put("listTag", list); 
				collPosts.save(doc);
			}
			/**/

			doc.put(tag,cont);
			collPosts.save(doc);
		}

		System.out.println(numWords+" palavras em "+tag);
		DBObject queryAccur = query.start("tags").is(tag).and(tag).greaterThan(0.001).get();
		//System.out.println(tag+":"+collPosts.getCount(queryAccur));
		//DBObject queryAccur = query.start("tags").size(5).and("listTag").is(tag).get();
		System.out.println(tag+" Tags:"+totalL);
		System.out.println(tag+" Tags Find:"+collPosts.getCount(queryAccur));

        mongoConn.close();
    }
	
}
