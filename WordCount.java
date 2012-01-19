import com.mongodb.*;

import java.util.regex.*;
import java.util.*;
import java.net.*;
import java.io.*;
import java.text.Normalizer;
 
public class WordCount {
  
	private static Mongo mongoConn;
	private static DB mongoDb;
	private static DBCollection collSWords, collWords, collPosts;
	
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
		collWords = mongoDb.getCollection("words_popular");
		collPosts = mongoDb.getCollection("posts");
		
		String mapContent =	"function(){ " +
							"   if (this.content)" +
							"	this.content.split(' ').forEach( " +
							"		function(word){ " +
							"			word.split('\\n').forEach( " +
							"				function(piece){ emit( piece , 1 ); } "+
							"			); "+
							"		} "+
							"	); "+
							"};";							

        String reduceWords = "function( key , values ){ "+
							"	var totCom = 0; " +
							"	values.forEach(function(value) {"+
							"		totCom += value; "+
							"   }); " +
							"	return totCom; "+
							"};";							
		
		QueryBuilder query = new QueryBuilder();
		DBObject docQuery = query.start("numComments").is(10).and("content").notEquals("").get();

        //MapReduceOutput output = collPosts.mapReduce(mapContent, reduceWords, "words_politica", MapReduceCommand.OutputType.REPLACE, docQuery);
		//DBCollection collResult = mongoDb.getCollection("authorWords");

		DBObject stopQuery, wordQuery;	

		String word;

		Scanner scan = new Scanner(System.in); 
		BasicDBObject doc = new BasicDBObject();

		BasicDBObject sortDoc = new BasicDBObject();
        sortDoc.put("value", -1);
		DBObject queryDoc = query.start("dot").notEquals(1).and("value").greaterThan(100).get();
		DBCursor cur = collWords.find(queryDoc).sort(sortDoc).limit(10);

		while(cur.hasNext()) {
			DBObject obj = cur.next();
			word = obj.get("_id").toString().replaceAll("\\W","");
			stopQuery = new BasicDBObject();
			stopQuery.put("word",word);
			//stopQuery = query.start("word").is(Pattern.compile(word,Pattern.CASE_INSENSITIVE)).get();
			//wordQuery = query.start("_id").is(Pattern.compile(word,Pattern.CASE_INSENSITIVE)).and("dot").is(1).get();
			
			if (collSWords.find(stopQuery).count()==0) { // && collSWords.find(wordQuery).count()==0) {
				System.out.println( word);			
				if (scan.nextInt() == 1) {
					doc = new BasicDBObject();
					doc.put("word", word);
					collSWords.insert(doc);
				}
			}
			obj.put("dot",1);
			collWords.save(obj);
		}

        mongoConn.close();
    }
	
}