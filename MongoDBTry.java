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

		String mapBlogs =	"function(){ " +
							"	emit( this.authorID , { count : 1 , comments : this.comments.length} ); "+
							"};";							
		
        String reduceAvg = "function( key , values ){ "+
							"	var posts = 0; var totCom = 0; " +
							"	for ( var i=0; i<values.length; i++ ) {"+
							"		posts += values[i].count; "+
							"		totCom += values[i].comments; "+
							"   } " +
							"	return Math.round(totCom/posts); "+
							"};";
		

		
		QueryBuilder query = new QueryBuilder();
		DBObject docQuery = query.start("comments").notEquals(new BasicDBList()).get();		
		
        MapReduceOutput output = collPosts.mapReduce(mapBlogs, reduceAvg, "temp", MapReduceCommand.OutputType.REDUCE, docQuery);
		DBCollection collResult = output.getOutputCollection();

		QueryBuilder mpQuery = new QueryBuilder();
		DBObject mpDoc = query.start("value").greaterThanEquals(2).get();
		
		long blogsPop = collResult.getCount(mpDoc);
		
		output.drop();
		
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