import com.mongodb.*;

import java.util.regex.*;
import java.util.*;
import java.net.*;
import java.io.*;
import java.text.Normalizer;
 
public class PageRank {
  
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
							"			if (comment.authorID!=idAuthor) {" +
							"				emit ( comment.authorID , { pr:0 , out:1 ,  outL:[ idAuthor ] } ); " +
							"				emit ( idAuthor , { pr:0.5 , out:0 , outL:[] } ); " +
							"			}"+
							"		} ); "+
							"	} "+
							"}; ";								

        String reduceAuthor = "function( key , values ){ "+
							"	var result = { pr:0 , out:0 , outL:[] }; " +
							"	values.forEach(function(value) {"+
							"		result.pr += value.pr; "+
							"		result.out += value.out; "+
							"		result.outL = result.outL.concat(value.outL); "+
							"   }); " +
							"	if (result.out) result.pr = 0.5*100 / result.out; "+
							"	return result; "+
							"};";	
							
		String mapAuthor2 =	"function(){ " +
							"	var prK = this.value.pr;"+
							"	this.value.outL.forEach ( function (value) { "+
							"		emit ( value , { pr:prK , out:0 , outL:[] } ); " +
							"	} ); "+
							"}; ";	
							

        String reduceAuthor2 = "function( key , values ){ "+
							"	var result = { pr:0 , out:0 , outL:[] }; " +
							"	values.forEach(function(value) {"+
							"		result.pr += value.pr; "+
							"		result.out += value.out; "+
							"		result.outL = result.outL.concat(value.outL); "+
							"   }); " +
							"	if (result.out) result.pr = result.pr / result.out; "+
							"	return result; "+
							"};";													
		
		QueryBuilder query = new QueryBuilder();
		DBObject docQuery = query.start("comments").notEquals(new BasicDBList()).and("content").notEquals("").get();		
		
		
        MapReduceOutput output = collPosts.mapReduce(mapAuthor, reduceAuthor, "pageRank_1", MapReduceCommand.OutputType.REPLACE, docQuery);
		DBCollection collResult = output.getOutputCollection();


        MapReduceOutput output2 = collResult.mapReduce(mapAuthor2, reduceAuthor2, "pageRank_2", MapReduceCommand.OutputType.REPLACE, null);
		DBCollection collResult2 = output2.getOutputCollection();
		
		//DBCollection collResult = mongoDb.getCollection("top10authors");

		QueryBuilder mpQuery = new QueryBuilder();
		DBObject mpDoc = query.start("value").greaterThan(50).get();
		
		BasicDBObject sortDoc = new BasicDBObject();
        sortDoc.put("value.pr", -1);

		DBCursor cur = collResult.find().sort(sortDoc).limit(10);
        while(cur.hasNext()) {
        	DBObject obj = cur.next();
        	System.out.println(obj.toString());
        }

        System.out.println("---------------------------------");

		cur = collResult2.find().sort(sortDoc).limit(10);
        while(cur.hasNext()) {
        	DBObject obj = cur.next();
        	System.out.println(obj.toString());
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