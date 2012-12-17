import nl.peterbloem.powerlaws.*;
import nl.peterbloem.util.*;

import com.mongodb.*;

import java.util.*;
import java.net.*;
import java.io.*;
import java.text.Normalizer;

public class PowerLaw {

	private static Mongo mongoConn;
	private static DB mongoDb;
	private static DBCollection coll;

    public static void main(String[] args) throws Exception {


		if (args.length!=1) {
		   System.out.println("Falta nome da colletion");
		   System.exit(1);
		}

		mongoConn = new Mongo( "localhost" , 27017 );
		mongoDb = mongoConn.getDB( "blogdb" );

		try {
			mongoDb.getCollectionNames();
		} catch (Exception e) {
			System.out.println("MongoDB Offline.");
			System.exit(1);
		}

		String collName = args[0];
		coll = mongoDb.getCollection("pageRank_2"+collName);

		List<Double> data = new ArrayList();
		Integer value = 0;
		DBCursor cur = coll.find();
        	while(cur.hasNext()) {
        		DBObject obj = cur.next();

			BasicDBList listComments = (BasicDBList)((BasicDBObject)obj.get("value")).get("outL");
        		if (listComments.size()>0) {
				//System.out.print(listComments.size()+",");
				value = listComments.size();
				data.add(value.doubleValue());
			}
        	}
		System.out.println("Count:"+coll.getCount());

	    	Continuous model = Continuous.fit(data).fit();
	    	double significance = model.significance(data, 100);
	        System.out.println( "significance:"+significance );

		mongoConn.close();

    }	
	
}
