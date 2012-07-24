import com.mongodb.*;

import java.util.regex.*;
import java.util.*;
import java.net.*;
import java.io.*;
import java.text.Normalizer;
 
public class TfIdf_KMeans {
  
	private static Mongo mongoConn;
	private static DB mongoDb;
	private static DBCollection collSWords, collWords, collPosts;
	
    public static void main(String[] args) throws Exception {		

		mongoConn = new Mongo( "localhost" , 27018 );
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
							"	tags = this.tags;"+
							"	this.title.replace(/[\\W\\d]/g,' ').replace(/\\s+/g,' ').split(' ').forEach( " +
							"		function(word){ " +
							"		tags.forEach(function(stag){ " +
							"			emit( {tag:stag.trim().toLowerCase(), term:word.trim().toLowerCase()}, 1 ); "+
							"		} ); }"+
							"	); "+
							"};";							

        String reduceWords = "function( key , values ){ "+
							"	var totCom = 0; " +
							"	values.forEach(function(value) {"+
							"		totCom += value; "+
							"   }); " +
							"	return totCom; "+
							"};";							
		
String mapDocK = "function() {"+
				" emit(this._id.id.replace('\"','').trim(), {word: this._id.word, count: this.value}) " +
				"};";


String redDocK = "function(key, values) {"+
					"var tot = 0;"+
					"values.forEach(function(value) {"+
						"  tot += value.count; "+
					"});"+
					"return { word: 0 , count: tot };"+
				"};";

String mapCorpusK = "function() {"+
				" emit(this._id.word, 1); " +
				"};";


	QueryBuilder query = new QueryBuilder();
	DBObject docQuery = query.start("numComments").is(10).and("content").notEquals("").get();
	docQuery = query.start("authorID").is("02172750966394283544").get();

	MapReduceOutput output = collPosts.mapReduce(mapContent, reduceWords, "tfidf_title", MapReduceCommand.OutputType.REPLACE, docQuery);
/*
	DBCollection collWords = mongoDb.getCollection("WordSubst");

	QueryBuilder query = new QueryBuilder();
	DBObject stopQuery, wordQuery;	

		String word;

		Scanner scan = new Scanner(System.in); 
		BasicDBObject doc = new BasicDBObject();

		BasicDBObject sortDoc = new BasicDBObject();
        	sortDoc.put("value", -1);
		DBObject queryDoc = query.start("subst").is(1).get();
		DBCursor cur = collWords.find(queryDoc).sort(sortDoc).limit(10);

		while(cur.hasNext()) {
			DBObject obj = cur.next();
			System.out.println(obj.get("_id").toString());
			
			if (scan.nextInt() == 1) {
				collWords.remove(obj);
			} else {
				obj.put("subst",0);
				collWords.save(obj);
			}
		}
*/
        mongoConn.close();
    }
	
}
