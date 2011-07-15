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
		
        String reduceDefault = "function( key , values ){ "+
							"	var total = 0; "+
							"	for ( var i=0; i<values.length; i++ ) "+
							"		total += values[i].count; "+
							"	return { count : total }; "+
							"};";
		
		BasicDBObject doc = new BasicDBObject();
		doc.put("blogID", "3942129895874784210");
		
		System.out.println("Posts: " + collPosts.count(doc));
		
        MapReduceOutput output = collPosts.mapReduce(mapMonth, reduceDefault, null, MapReduceCommand.OutputType.INLINE, doc);
		int cont=0;
		for (DBObject mapReduceObject : output.results()) {
            System.out.println(mapReduceObject);
        }
		
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