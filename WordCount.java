import com.mongodb.*;

import java.util.regex.*;
import java.util.*;
import java.net.*;
import java.io.*;
import java.text.Normalizer;
 
public class WordCount {
  
	private static Mongo mongoConn;
	private static DB mongoDb;
	private static DBCollection collPosts;
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
		
		collPosts = mongoDb.getCollection("posts");
		collSWords = mongoDb.getCollection("stopWords");
		
		String mapTags = 	"function(){ "+
							"	this.tags.forEach( "+
							"		function(tag){ "+
							"			emit( tag , 1 ); "+
							"		} "+
							"	); "+
							"};";

		String mapWords =	"function(){ " +
							"	this.content.split(' ').forEach( " +
							"		function(word){ " +
							"			word.split('\\n').forEach( " +
							"				function(piece){ emit( piece , 1); } "+
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
		DBObject docQuery = query.start("comments").notEquals(new BasicDBList()).and("content").notEquals("").and("authorID").is("16235025724127440347").get();		
		
        //MapReduceOutput output = collPosts.mapReduce(mapWords, reduceWords, "authorWords", MapReduceCommand.OutputType.REPLACE, docQuery);
		DBCollection collResult = mongoDb.getCollection("authorWords");
		
		BasicDBObject sortDoc = new BasicDBObject();
        sortDoc.put("value", -1);

		Scanner scan = new Scanner(System.in);  

		DBCursor cur = collResult.find().sort(sortDoc).limit(10);
        while(cur.hasNext()) {
        	DBObject obj = cur.next();

			BasicDBObject doc = new BasicDBObject();
			doc.put("word", obj.get("_id"));   	

        	if (collSWords.find(doc).count()==0) {
        		System.out.print( obj.get("_id").toString() + "?");
        		if (scan.nextInt() == 1) {
        			System.out.println("SW");
        		}
        	}
        }

        mongoConn.close();
    }
	
}