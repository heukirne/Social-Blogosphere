import com.mongodb.*;

import java.util.regex.*;
import java.util.*;
import java.net.*;
import java.io.*;
import java.text.Normalizer;
 
public class MongoDBTry {
  
	private static Mongo mongoConn;
	private static DB mongoDb;
	private static DBCollection collPosts;
	
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
		collPosts.ensureIndex("postID");
		collPosts.ensureIndex("blogID");				
		collPosts.ensureIndex("authorID");

		registerShutdownHook();
		
		String mapTags = 	"function(){ "+
							"	this.tags.forEach( "+
							"		function(tag){ "+
							"			emit( tag , { count : 1 } ); "+
							"		} "+
							"	); "+
							"};";
		
		String mapMonth = 	"function(){ "+
							"	var d = new Date(this.published); "+
							"	emit( d.getMonth() , { count : 1 } ); "+
							"};";
							
		String mapContent =	"function(){ " +
							"	this.content.split(' ').forEach( " +
							"		function(word){ " +
							"			word.split('\\n').forEach( " +
							"				function(piece){ emit( piece , { count : 1 } ); } "+
							"			); "+
							"		} "+
							"	); "+
							"};";	

		String mapAuthor =	"function(){ " +
							" if (this.content) " +
							"	if (this.content.indexOf('politica')>0) { emit( this.authorID , this.comments.length ); } "+
							"};";							
		
        String reduceAvg = "function( key , values ){ "+
							"	var posts = 0; var totCom = 0; " +
							"	for ( var i=0; i<values.length; i++ ) {"+
							"		posts += values[i].count; "+
							"		totCom += values[i].comments; "+
							"   } " +
							"	return Math.round(totCom/posts); "+
							"};";

        String reduceAuthor = "function( key , values ){ "+
							"	var totCom = 0; " +
							"	for ( var i=0; i<values.length; i++ ) {"+
							"		totCom += values[i]; "+
							"   } " +
							"	return totCom; "+
							"};";							
		

		
		QueryBuilder query = new QueryBuilder();
		DBObject docQuery = query.start("comments").notEquals(new BasicDBList()).and("content").notEquals("").get();		
		
		
        MapReduceOutput output = collPosts.mapReduce(mapAuthor, reduceAuthor, "atuhorComp", MapReduceCommand.OutputType.REPLACE, docQuery);
		DBCollection collResult = output.getOutputCollection();
		
		//DBCollection collResult = mongoDb.getCollection("atuhorComp");

		QueryBuilder mpQuery = new QueryBuilder();
		DBObject mpDoc = query.start("value").greaterThanEquals(10).get();
		
		BasicDBObject sortDoc = new BasicDBObject();
        sortDoc.put("value", -1);

		DBCursor cur = collResult.find(mpDoc).sort(sortDoc);

        while(cur.hasNext()) {
            System.out.println(cur.next());
            break;
        }

		//output.drop();
		
        shutdown();
    }
    private static void shutdown()
    {
		System.out.println( "Shutting down database ..." );
		mongoConn.close();
    }
	
    private static void registerShutdownHook()
    {
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                shutdown();
            }
        } );
    }
}