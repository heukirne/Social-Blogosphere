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
							"				emit ( comment.authorID , { pr:0 , outL:[ idAuthor ] } ); " +
							"			}"+
							"		} ); "+
							"	} "+
							"}; ";								

        String reduceAuthor = "function( key , values ){ "+
							"	var result = { pr:0.5 , outL:[] }; " +
							"	values.forEach(function(value) {"+
							"		result.outL = result.outL.concat(value.outL); "+
							"   }); " +
							"	return result; "+
							"};";	
							

		String mapAuthor2 =	"function(){ " +
							"	var prK = this.value.pr/this.value.outL.length;"+
							"	this.value.outL.forEach ( function (value) { "+
							"		emit ( value , { pr:prK , outL:[] } ); " +
							"	} ); "+
							"	emit ( this._id , this.value );" +
							"}; ";	
							

        String reduceAuthor2 = "function( key , values ){ "+
							"	var result = { pr:0 , outL:[] }; " +
							"	values.forEach(function(value) {"+
							"		result.pr += value.pr; "+
							"		result.outL = result.outL.concat(value.outL); "+
							"   }); " +
							"	return result; "+
							"};";													
		
		QueryBuilder query = new QueryBuilder();
		DBObject docQuery = query.start("comments").notEquals(new BasicDBList()).and("content").notEquals("").get();		
		
		
        //MapReduceOutput output = collPosts.mapReduce(mapAuthor, reduceAuthor, "pageRank_1", MapReduceCommand.OutputType.REPLACE, docQuery);
		DBCollection collResult = mongoDb.getCollection("pageRank_1");

        MapReduceOutput output2 = collResult.mapReduce(mapAuthor2, reduceAuthor2, "pageRank_2", MapReduceCommand.OutputType.REPLACE, null);
		DBCollection collResult2 = output2.getOutputCollection();

		BasicDBObject sortDoc = new BasicDBObject();
        sortDoc.put("value.pr", -1);
        
        DBCursor cur = collResult2.find().sort(sortDoc).limit(10);
        int hash = cursorInt(cur);
		int hash2 = 0;
		for (int i=0; i<10; i++) {
	        output2 = collResult2.mapReduce(mapAuthor2, reduceAuthor2, "pageRank_2", MapReduceCommand.OutputType.REPLACE, null);
			collResult2 = output2.getOutputCollection();

	        cur = collResult2.find().sort(sortDoc).limit(10);
	        hash2 = cursorInt(cur);

	        if (hash == hash2) break;
	        else hash = hash2;
		}
		
        shutdown();
    }

    private static int cursorInt(DBCursor cur) {
        int sum = 0;
        while(cur.hasNext()) {
    		DBObject obj = cur.next();
    		for(char num: obj.get("_id").toString().toCharArray() ) 
    			sum += Character.getNumericValue(num);
    	}	
    	System.out.println(sum);
    	return sum;
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