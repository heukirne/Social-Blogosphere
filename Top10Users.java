import com.mongodb.*;

import java.util.regex.*;
import java.util.*;
import java.net.*;
import java.io.*;
import java.text.Normalizer;
 
public class Top10Users {
  
	private static Mongo mongoConn;
	private static DB mongoDb;
	private static DBCollection collPosts;
	
    public static void main(String[] args) throws Exception {		

        String word = "futebol";

		mongoConn = new Mongo( "localhost" , 27017 );
		mongoDb = mongoConn.getDB( "blogdb" );
		
		try {
			mongoDb.getCollectionNames();
		} catch (Exception e) {
			System.out.println("MongoDB Offline.");
			System.exit(1);
		}
		
		collPosts = mongoDb.getCollection("posts");

		registerShutdownHook();

		String mapAuthor =	"function(){ " +
							" if (this.content) " +
							"	if (this.content.indexOf('" + word + "')>0) {"+
							"		idAuthor = this.authorID;"+
							"		this.comments.forEach ( function (comment) { "+
							"			emit ( comment.authorID , 1 ); " +
							"			emit ( idAuthor , 1 ); " +
							"		} ); "+
							"	} "+
							"}; ";							

        String reduceAuthor = "function( key , values ){ "+
							"	var total = 0; " +
							"	for ( var i=0; i<values.length; i++ ) {"+
							"		total += values[i]; "+
							"   } " +
							"	return total; "+
							"};";							
		
		QueryBuilder query = new QueryBuilder();
		DBObject docQuery = query.start("comments").notEquals(new BasicDBList()).and("content").notEquals("").get();		
		
		
        MapReduceOutput output = collPosts.mapReduce(mapAuthor, reduceAuthor, "top10authors", MapReduceCommand.OutputType.REPLACE, docQuery);
		DBCollection collResult = output.getOutputCollection();
		
		//DBCollection collResult = mongoDb.getCollection("top10authors");

		QueryBuilder mpQuery = new QueryBuilder();
		DBObject mpDoc = query.start("value").greaterThan(50).get();
		
		BasicDBObject sortDoc = new BasicDBObject();
        sortDoc.put("value", -1);

		int contCur = 0;
		DBCursor cur = collResult.find(mpDoc).sort(sortDoc);
        while(cur.hasNext()) {
        	DBObject obj = cur.next();
        	System.out.println(obj.toString());
        	if (contCur++ > 10) break;
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