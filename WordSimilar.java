import com.mongodb.*;

import java.util.regex.*;
import java.util.*;
import java.net.*;
import java.io.*;
import java.text.Normalizer;
 
public class WordSimilar {
  
	private static Mongo mongoConn;
	private static DB mongoDb;
	private static DBCollection collSWords, collPosts;
	
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
		docQuery = query.start("content").is(Pattern.compile("politica",Pattern.CASE_INSENSITIVE)).and("numComments").greaterThan(5).get();

        //MapReduceOutput output = collPosts.mapReduce(mapContent, reduceWords, "words_politica", MapReduceCommand.OutputType.REPLACE, docQuery);
		DBCollection collResult = mongoDb.getCollection("words_politica");

		DBObject stopQuery;	

		String word;

		BasicDBObject doc = new BasicDBObject();

		BasicDBObject sortDoc = new BasicDBObject();
        sortDoc.put("value", -1);

		DBCursor cur = collResult.find().sort(sortDoc).limit(200);

		while(cur.hasNext()) {
			DBObject obj = cur.next();
			word = obj.get("_id").toString().replaceAll("\\W","");

			stopQuery = query.start("word").is(Pattern.compile(word,Pattern.CASE_INSENSITIVE)).get();
			
			//System.out.println(word);
			if (collSWords.find(stopQuery).count()>0) {
				collResult.remove(obj);
			}

		}

        mongoConn.close();
    }
	
}