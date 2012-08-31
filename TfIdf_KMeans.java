import com.mongodb.*;

import java.util.regex.*;
import java.util.*;
import java.net.*;
import java.io.*;
import java.text.Normalizer;
 /*
226m Tag TF, 5m Tag N, 13m Tag K, 150m Join N, 60m Join K, 30m TFIDF
18m Doc TF, 1m Doc N, 3m Doc K, 20m Join N, 15m Join K, 8m TFIDF
 */
public class TfIdf_KMeans {
  
	private static Mongo mongoConn;
	private static DB mongoDb;
	private static DBCollection collTFIDFN, collTFIDF, collPosts;
	
    public static void main(String[] args) throws Exception {		

		mongoConn = new Mongo( "localhost" , 27017 );
		mongoDb = mongoConn.getDB( "blogdb" );
		
		try {
			mongoDb.getCollectionNames();
		} catch (Exception e) {
			System.out.println("MongoDB Offline.");
			System.exit(1);
		}
			
		collTFIDF = mongoDb.getCollection("tfidf_word5");
		collPosts = mongoDb.getCollection("posts");
		
		String mapContentTag =	"function(){ " +
							"   if (this.content) {" +
							"	tags = this.tags;"+
							"	this.content.replace(/[\\W\\d]/g,' ').replace(/\\s+/g,' ').split(' ').forEach( " +
							"		function(word){ " +
							"		tags.forEach(function(stag){ " +
							"			if (word.trim().length > 0 && stag.trim().length > 0) " +
							"			emit( {tag:stag.toLowerCase().replace(/[\\W\\d]/g,' ').replace(/\\s+/g,' ').trim(), term:word.trim().toLowerCase()}, 1 ); "+
							"		} ); }"+
							"	); } "+
							"};";							

		String mapContentWord =	"function(){ " +
							" var postID = this.postID; " +
							"	this.content.replace(/[\\W\\d]/g,' ').replace(/\\s+/g,' ').split(' ').forEach( " +
							"		function(word){ " +
							"			emit( {id:postID, term:word.trim().toLowerCase()}, 1 ); "+
							"   } );"+
							"};";	

        String reduceWords = "function( key , values ){ "+
							"	var totCom = 0; " +
							"	values.forEach(function(value) {"+
							"		totCom += value; "+
							"   }); " +
							"	return totCom; "+
							"};";							
		
String mapDocN = "function() {"+
				" emit(this._id.id, this.value) " +
				"};";


String redDocK = "function(key, values) {"+
					"var tot = 0;"+
					"values.forEach(function(value) {"+
						"  tot += value.count; "+
					"});"+
					"return { word: 0 , count: tot };"+
				"};";

String mapCorpusm = "function() {"+
				" emit(this._id.term, 1); " +
				"};";


	QueryBuilder query = new QueryBuilder();
	DBObject docQuery = query.start("numComments").is(10).and("content").notEquals("").get();
	docQuery = query.start("tags").size(5).get();

	MapReduceOutput output = collTFIDF.mapReduce(mapDocN, reduceWords, "tfidf_word5N", MapReduceCommand.OutputType.REPLACE, null);



/*
db.tfidf_word5.ensureIndex({"_id.id":1}); 
db.tfidf_word5.ensureIndex({"_id.term":1}); 

db.tfidf_word5N.find().forEach(function(d){db.tfidf_word5.update({"_id.id":d._id},{$set:{N:d.value}},false,true)});
db.tfidf_word5m.find().forEach(function(d){db.tfidf_word5.update({"_id.term":d._id,m:{$exists:false}},{$set:{m:d.value}},false,true)});

D = db.tfidf_word5N.count();
db.tfidf_word5.find().forEach(function(d){d.tfidf = (d.value/d.N)*Math.log(D/d.m); db.tfidf_word5.save(d)});


docQuery = query.start("authorID").exists(false).get();
DBObject queryDoc;
DBCursor curDoc;
DBObject obj, objList;
DBCursor cur = collTFIDFN.find(docQuery);
BasicDBObject sortDoc = new BasicDBObject();
sortDoc.put("tfidf", -1);
BasicDBList list = new BasicDBList();

while(cur.hasNext()) {
	obj = cur.next();

	/*queryDoc = query.start("_id.id").is(obj.get("_id").toString()).get();
	curDoc = collTFIDF.find(queryDoc).sort(sortDoc).limit(10);
	
	list = new BasicDBList();
	while(curDoc.hasNext()) {
		objList = curDoc.next();
		list.add(list.size(), ((DBObject)objList.get("_id")).get("term").toString());
	}
	obj.put("list", list); 

	//System.out.println(obj.get("_id").toString());
	queryDoc = query.start("postID").is(obj.get("_id").toString()).get();
	objList = collPosts.findOne(queryDoc);
	//System.out.println(objList.get("authorID").toString());
	obj.put("authorID",objList.get("authorID").toString());


	collTFIDFN.save(obj);
}

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
