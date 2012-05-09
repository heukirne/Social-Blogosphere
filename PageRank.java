import com.mongodb.*;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.DriverManager;

import java.util.regex.*;
import java.util.*;
import java.net.*;
import java.io.*;
import java.text.Normalizer;
 
public class PageRank {
  
	private static Mongo mongoConn;
	private static DB mongoDb;
	private static DBCollection collPosts;
	public static String myConnString = "jdbc:mysql://localhost/bloganalysis?user=&password=";
        public static Connection mysqlConn;
        public static Statement myStm;
	
    public static void main(String[] args) throws Exception {		

		if (args.length!=1) {
		   System.out.println("Falta nome da colletion");
		   System.exit(1);
		}

Properties configFile = new Properties();
configFile.load( new FileInputStream("my_config.properties"));
myConnString = configFile.getProperty("MYCONN");

		mongoConn = new Mongo( configFile.getProperty("MYHOST") , 27017 );
		mongoDb = mongoConn.getDB( "blogdb" );

		try {
			mongoDb.getCollectionNames();
		} catch (Exception e) {
			System.out.println("MongoDB Offline.");
			System.exit(1);
		}

		try {
			mysqlConn = DriverManager.getConnection(myConnString);
			myStm = mysqlConn.createStatement();
			myStm.executeQuery("set wait_timeout = 7200");
		} catch (Exception e) {
			System.out.println("MySQL Offline.");
			System.exit(1);
		}
	
	String collName = args[0];
	System.out.println(collName);
	
	String mapAuthor =	"function(){ " +
					"  idAuthor = this.authorID;"+
					"  emit ( this.authorID, { post:1, comment:0, outL:[] } );"+
					"  this.comments.forEach ( function (comment) { "+
					"      if (comment.authorID!=idAuthor) {" +
					"         emit ( comment.authorID , { post:0, comment:1 , outL:[ idAuthor ] } ); " +
					"      }"+
					"  } ); "+
					"}; ";								

        String reduceAuthor = "function( key , values ){ "+
					"	var result = { post:0, comment:0, outL:[] }; " +
					"	values.forEach(function(value) {"+
					"	   var bSet = false;"+
					"	   result.post += value.post;"+
					"          result.comment += value.comment;"+
					"	   /*value.outL.forEach(function(iV){"+
					"		result.outL.forEach(function(iR){"+
					"		   if (iV[0]==iR[0]) { "+
					"		      iR[1]+=iV[1]; bSet = true;"+
					"		   }"+
					"		});"+
					"	   });"+
					"	   if (!bSet)*/ result.outL = result.outL.concat(value.outL); "+
					"       }); " +
					"	return result; "+
					"};";	
							

	/** ### Generate PageRank Table ### /

	collPosts = mongoDb.getCollection("posts");
	QueryBuilder query = new QueryBuilder();
	//DBObject docQuery = query.start("tags").is(collName).get();
	DBObject docQuery = query.start("numComments").greaterThan(0).get();
	//docQuery = query.start("tags").is("politica").and("authorID").is("15379833583638166492").get();
	//String fiat = "siena|fiat 500|novo uno|stilo|mille|doblo|abs|airbag|seguro|estabilidade|freio|compacto|estilo|dianteira|roda|cores|liga leve|farol|pintura|calotas|acabamente|espaco|painel|volante|bancos|porta-malas|ar-condicionado|som|cabine|confortavel|preco|caro|custo|manutencao|garantia|revenda|motor|cambio|automatico|potencia|torque|manual|turbo|velocidade|suspensao|tracao|transmissao|forte|mecanica|rapido|fraco|potente|aceleracao|giros|rotacoes|performance|gasolina|litros|combustivel|alcool|economico|beberrao";
	//docQuery = query.start("content").is(Pattern.compile(fiat,Pattern.CASE_INSENSITIVE)).get();
	//docQuery = query.start("content").is(Pattern.compile(fiat,Pattern.CASE_INSENSITIVE)).and("authorID").is("15379833583638166492").get();
    	MapReduceOutput output = collPosts.mapReduce(mapAuthor, reduceAuthor, "pageRank_"+collName, MapReduceCommand.OutputType.REPLACE ,docQuery);
	/**/

	/** ### Populate global author info and Initial PR ### /

	DBCollection collResult = mongoDb.getCollection("pageRank_"+collName);
	DBCollection collAuthor = mongoDb.getCollection("authorAll");

	BasicDBObject doc = new BasicDBObject();
	Double prPost = 0d;
	Double prComment = 0d;
        DBCursor cur = collResult.find();
        while(cur.hasNext()) {
                BasicDBObject obj = (BasicDBObject)cur.next();

		doc = new BasicDBObject();
		doc.put("_id",obj.get("_id").toString());
		BasicDBObject author = (BasicDBObject)collAuthor.findOne(doc);
		//BasicDBObject objValue = (BasicDBObject)obj.get("value");

		//prPost = objValue.getDouble("post");
		prPost = ((BasicDBObject)author.get("value")).getDouble("post");
		//prComment = objValue.getDouble("comment");
		prComment = ((BasicDBObject)author.get("value")).getDouble("comment");

		if (prPost.isNaN()) { prPost = 0d; }
		if (prComment.isNaN()) { prComment = 0d; }

		obj.put("Tpost",prPost);
		obj.put("Tcomment",prComment);
		//objValue.put("pr",1d);
		//obj.append("value",objValue);

		collResult.save(obj);
        }
	/**/

        String mapAuthor2 ="function(){ " +
                           "    var prP = this.Tpost ? (this.value.post*this.value.post)/this.Tpost : 0;"+
			   "	var prC = this.Tcomment ? (this.value.comment*this.value.comment)/this.Tcomment : 0;"+
			   "    var prK = (prP+prC)/this.value.outL.length;"+
                           "    this.value.outL.forEach ( function (value) { "+
                           "        emit ( value , { pr:prK , outL:[], prOld:0 } ); " +
                           "    } ); "+
                           "    emit ( this._id , { pr: 0 , outL: this.value.outL, prOld :0 } );" +
                           "}; ";


       String mapAuthor3 =     "function(){ " +
                               "       var prK = this.value.pr/this.value.outL.length;"+
                               "       this.value.outL.forEach ( function (value) { "+
                               "               emit ( value , { pr:prK , outL:[] , prOld: 0 } ); " +
                               "       } ); "+
                               "       if (this.value.outL.length + prK > 0) { emit ( this._id , { pr: 0 , outL: this.value.outL , prOld: this.value.pr } ); }" +
                               "}; ";

        String reduceAuthor2 = "function( key , values ){ "+
                               "       var result = { pr:0 , outL:[] , prOld:0 }; " +
                               "       values.forEach(function(value) {"+
                               "               result.pr += value.pr; "+
                               "               result.outL = result.outL.concat(value.outL); "+
			       "		result.prOld += value.prOld;" +
                               "   }); " +
                               "       return result; "+
                               "};";

	/** ### Execute PageRank Iteration ### */	

	DBCollection collResultP = mongoDb.getCollection("pageRank_"+collName);
    	MapReduceOutput output2 = collResultP.mapReduce(mapAuthor2, reduceAuthor2, "pageRank_2"+collName, MapReduceCommand.OutputType.REPLACE, null);
	DBCollection collResult2 = mongoDb.getCollection("pageRank_2"+collName);
	//MapReduceOutput output2;

	BasicDBObject sortDocP = new BasicDBObject();
        sortDocP.put("value.pr", -1);
        
        DBCursor curP;
        double mse = 0;
	for (int i=0; i<20; i++) {
		output2 = collResult2.mapReduce(mapAuthor3, reduceAuthor2, "pageRank_2"+collName, MapReduceCommand.OutputType.REPLACE, null);
		collResult2 = output2.getOutputCollection();

	        curP = collResult2.find().sort(sortDocP).limit(100);
	        mse = checkSum(curP);

	        if (mse < 10d) break;
		//System.out.print(".");
	}
	System.out.println("PR-Done");
	/**/

        /** Populate Valid Users /
        DBCollection collResult = mongoDb.getCollection("pageRank_2"+collName);

        DBCursor cur = collResult.find();
        while(cur.hasNext()) {
                DBObject obj = cur.next();

                myStm.executeQuery("SELECT Interesses FROM author WHERE profileID = '" + obj.get("_id").toString() + "';");
                ResultSet rs = myStm.getResultSet();
                if (rs.next()) {
                        if (rs.getString("Interesses") != null) {
                                if (rs.getString("Interesses").indexOf(collName)!=-1) {
                                        obj.put("valid",1);
                                } else {
                                        obj.put("valid",0);
                                }
                                collResult.save(obj);
                        }
                }

        }
	System.out.println("Populate Valid");
        /**/

	System.out.println(collName);
	/** ### Validate Results ### /

        DBCollection collResultV = mongoDb.getCollection("pageRank_2"+collName);

        BasicDBObject docValid = new BasicDBObject();
        docValid.put("valid", 1);
	
        BasicDBObject docInvalid = new BasicDBObject();
        docInvalid.put("valid", 0);  

	long valid = collResultV.find(docValid).count();
	long invalid = collResultV.find(docInvalid).count();
	System.out.println("Valid:,"+valid);
	System.out.println("Total:,"+(valid+invalid));
	System.out.println("Dataset:,"+collResultV.find().count());

        BasicDBObject sortDoc = new BasicDBObject();
        sortDoc.put("value.pr", -1);
        docValid.put("valid", new BasicDBObject("$exists", true));	

	DBCursor curV = collResultV.find(docValid).sort(sortDoc).limit(100);
	valid = 0;
	int cont = 0;
	System.out.println("Valid,Top#");
	while(curV.hasNext()) {
		BasicDBObject obj = (BasicDBObject)curV.next();
		if (obj.containsField("valid")) {
			if (obj.getInt("valid")==1) {
				valid++;
			}
			cont++;
			
			if ((cont%10)==0)
				System.out.println(valid);
		}
	}


	/**/

	System.out.println("Done");
        mongoConn.close();
	try {
        	myStm.close();
        } catch (Exception ex) {}

    }

    private static double checkSum(DBCursor cur) {
        double mEr = 0;
	double r1 = 0;
	double r2 = 0;
        while(cur.hasNext()) {
    		DBObject obj = cur.next();
    		/*for(char num: obj.get("_id").toString().toCharArray() ) 
    			sum += Character.getNumericValue(num);
    		*/
		r1 = ((BasicDBObject)obj.get("value")).getDouble("pr");
		r2 = ((BasicDBObject)obj.get("value")).getDouble("prOld"); 
		mEr += Math.sqrt(Math.pow(r1-r2,2)); 
	}
	mEr = mEr/100;	
    	System.out.println(mEr);
    	return mEr;
    }

}
