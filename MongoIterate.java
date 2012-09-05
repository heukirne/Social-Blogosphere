import com.mongodb.*;

import com.google.gdata.client.Query;
import com.google.gdata.client.blogger.BloggerService;
import com.google.gdata.data.*;
import com.google.gdata.util.ServiceException;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.DriverManager;

import org.apache.lucene.analysis.br.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.analysis.snowball.*;
import org.apache.lucene.util.*;

import java.util.regex.*;
import java.util.*;
import java.net.*;
import java.io.*;
import java.text.Normalizer;
import java.text.SimpleDateFormat;
import java.util.concurrent.*;
 
public class MongoIterate {
 
	public static String myConnString = "jdbc:mysql://localhost/bloganalysis?user=&password=";
	public static final int mongoPort = 27017;
	public static String mongoHost = "localhost";
	public static final int numCrawler = 2;
	public static Mongo mongoConn;
	public static DB mongoDb;
	public static DBCollection collPosts;
	public static Connection mysqlConn;
	public static Statement myStm;
	private final static String[] arStopWords = {"http", "www", "par", "no", "ser", "muit", "tem", "foi", "pod", "faz", "ja", "sao", "voc", "ate", "pel", "dia", "eu", "bem", "fic", "nov", "so", "pass", "ter", "for", "part", "sab", "vez", "cas", "aqu", "com", "primeir", "me", "dev", "temp", "deix", "grand", "algum", "melhor", "noss", "cont", "hoj", "vai", "anos", "sempr", "pesso", "trabalh", "la", "segund", "agor", "vid", "fal", "form", "minh", "pra", "conhec", "comec", "cois", "diz", "ond", "maior", "pouc", "cheg", "dess", "cad", "mund", "quer", "encontr", "meu", "ver", "bom", "apen", "diss", "mei", "esta", "lev", "gost", "era", "volt", "tenh", "estav", "ultim", "precis", "dois", "feit", "ano", "nad", "acontec", "fot", "alguns", "brasil", "marc", "estar", "post", "tant", "final", "esper", "mostr", "public", "viv", "tao", "amig", "hor", "segu", "cham", "sent", "histor", "dias", "dar", "num", "moment", "seman", "apresent", "acab", "mes", "ai", "pens", "torn", "parec", "receb", "coloc", "nom", "cri", "blog", "tinh", "continu", "tom", "consegu", "font", "vem", "lad", "cert"};
	public static Set<String> stopStemmWords;
	public static Analyzer analyzer;
	
    public static void main(String[] args) throws Exception {		

Properties configFile = new Properties();
configFile.load( new FileInputStream("my_config.properties"));
myConnString = configFile.getProperty("MYCONN");
mongoHost = configFile.getProperty("MYHOST");

		mongoConn = new Mongo( mongoHost , mongoPort );
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
		
		stopStemmWords = new HashSet<String>(Arrays.asList(arStopWords));
		analyzer = new BrazilianAnalyzer(Version.LUCENE_36);

		collPosts = mongoDb.getCollection("posts");
		collPosts.ensureIndex("postID");
		collPosts.ensureIndex("blogID");
		collPosts.ensureIndex("authorID");	

		getBlogs();

		System.out.println("Number Posts: " + collPosts.getCount());

		Thread.sleep(1000); //For cleaning mongo cursos 

		mongoConn.close();
        myStm.close();
    }
	
	public static void getBlogs() throws Exception 
	{

		BlockingQueue<String> queue = new ArrayBlockingQueue<String>(numCrawler*4);

		CrawlerM[] crawler = new CrawlerM[numCrawler];
		for (int i=0; i<crawler.length; i++) {
			crawler[i] = new CrawlerM(queue);
			crawler[i].start();
		}

		ResultSet rs = null;
		while(true)
		{
			myStm.executeQuery("SELECT blogID FROM blogBR WHERE Get=0 ORDER BY RAND() DESC LIMIT 10");
			rs = myStm.getResultSet();
			try {
				if (!rs.first()) break;
				while (rs.next()) {
            		if (!queue.offer(rs.getString("blogID"),60,TimeUnit.SECONDS)) {
                    		System.out.println("Offer.Timeout");
            		}
				}
			} catch (Exception e) {}

		}

		queue.clear();
	    for (int i=0; i<crawler.length; i++)
	        queue.put(CrawlerM.NO_MORE_WORK);		
		for (int i=0; i<crawler.length; i++)
			crawler[i].join();
		
	}
	
	private static String[] getBlogFromMongo() {

		DBCollection collBlogs = mongoDb.getCollection("blogStats");

		QueryBuilder query = new QueryBuilder();
		DBObject docUnset = query.start("dot").notEquals(1).get();

		BasicDBObject sortDoc = new BasicDBObject();
        	sortDoc.put("value.posts", -1);

		Random generator = new Random();
		int r = generator.nextInt(50);

		DBCursor cur = collBlogs.find(docUnset).sort(sortDoc).limit(52).skip(r);
		DBObject obj = null ;
        	if(cur.hasNext()) 
         		obj = cur.next();
        	else
        		return null;

		String[] blogID = { obj.get("_id").toString() };
		
		return blogID;

	}

	public static String Stem(String text, Analyzer analyzer, Set<String> stopStemmWords){
        StringBuffer result = new StringBuffer();
        if (text!=null && text.trim().length()>0){

        	text = text.replaceAll("[\\W\\d_]"," ").replaceAll("\\s+"," "); //need to remove underline "no"
            StringReader tReader = new StringReader(text);
            TokenStream tStream = analyzer.tokenStream("contents", tReader);
            TermAttribute term = tStream.addAttribute(TermAttribute.class);

            try {
                while (tStream.incrementToken()){
                    if (!stopStemmWords.contains(term.term()) && term.term().length() > 2) {
                    	result.append(term.term());
                    	result.append(" ");
                    }
                }
            } catch (IOException ioe){
                System.out.println("Error: "+ioe.getMessage());
            }
        }

        // If, for some reason, the stemming did not happen, return the original text
        if (result.length()==0)
            result.append(text);
        return result.toString().trim();
		}

}

class CrawlerM extends Thread {

	private BloggerService myService;
	private Mongo mongoConn;
	private DB mongoDb;
	private DBCollection collPosts;	
	private int r;
	private String blog;
	
	public static Connection mysqlConn;
	public static Statement myStm;

	static final String NO_MORE_WORK = new String("");

	BlockingQueue<String> q;

    CrawlerM(BlockingQueue<String> q) {
    	this.q = q;
		try {
			Random generator = new Random();
			r = generator.nextInt(100);

			myService = new BloggerService("Mongo-BlogFeed-"+r);
			//myService.setReadTimeout(3000);

			mysqlConn = DriverManager.getConnection(MongoIterate.myConnString);
			myStm = mysqlConn.createStatement();
			myStm.executeQuery("set wait_timeout = 7200");

			mongoConn = new Mongo( MongoIterate.mongoHost , MongoIterate.mongoPort );
			mongoDb = mongoConn.getDB( "blogdb" );
			collPosts = mongoDb.getCollection("posts");   
		} catch (Exception e) {
			System.out.println(r+"bye:" + e.getMessage());
		}
		   
    }
    public void run() {
    	while (true) {

	    	try { 
				String blog = q.take();

                if (blog == NO_MORE_WORK) {
                    break;
                }
				
				myStm.executeUpdate("UPDATE blogBR SET Get = 1 WHERE blogID = '" + blog + "' LIMIT 1");
				getPosts(blog);

				System.out.println("Finish("+r+")");
			} catch (Exception e) {
				System.out.println(r+"runEx:" + e.getMessage()+">"+blog);
			}

		}

		System.out.println("Bye("+r+")");
		mongoConn.close();
		try { myStm.close(); } catch (Exception e) {}
    }

    private Feed feedQuery(Query myQuery) {
    	
		Feed resultFeed = new Feed();

		for (int i=0; i<=3; i++) {
			try {
				Thread.sleep(100);
				resultFeed = myService.query(myQuery, Feed.class);
				break;
			} catch (MalformedURLException e) {
				System.out.println(r+"MalformEx:"+ e.getMessage()+">"+blog);
			} catch (IOException e) {
				System.out.println(r+"IOEx:"+ e.getMessage()+">"+blog);
			} catch (ServiceException e) {
				System.out.println(r+"ServcEx: "+ e.getMessage()+">"+blog);
				if (e.getMessage().matches(".*Bad.*")) break;
				if (e.getMessage().matches(".*Not Found.*")) break;
			} catch (Exception e) {
				System.out.println(r+"feedEx: " + e.getMessage()+">"+blog);
			}
		}

		return resultFeed;

    }

	private boolean getPosts(String blogUri) throws Exception {			
				
		URL feedUrl = new URL("http://www.blogger.com/feeds/" + blogUri + "/posts/default");
		Query myQuery = new Query(feedUrl);
		
		DateTime dtMin = DateTime.parseDate("2012-05-01");
        DateTime dtMax = DateTime.parseDate("2012-07-30");

		myQuery.setPublishedMin(dtMin);	
		myQuery.setPublishedMax(dtMax);
		myQuery.setMaxResults(25);

		Feed resultFeed = feedQuery(myQuery);
		
		int count = 1;
		int size = resultFeed.getTotalResults();

		do {
			if (size==0) break;
			myQuery.setStartIndex(count);
			if (count>1) resultFeed = feedQuery(myQuery);

			System.out.println("["+r+"]"+blogUri+"("+count+"/"+size+")");

			for (Entry entry : resultFeed.getEntries()) {
				String postID = "";
				try {
					postID = entry.getSelfLink().getHref().replace("http://www.blogger.com/feeds/","").replace("posts/default/","");					
				} catch (Exception e) {
					continue;
				}
				if (entry.getAuthors().get(0).getUri()!=null) {
					setMongoPost(entry);
				}
				count++; 
			}

		} while (count <= size);

		return true;
	}
	
	private void getComments(final String postUri) throws Exception {

		URL feedUrl = new URL("http://www.blogger.com/feeds/" + postUri + "/comments/default");
		Query myQuery = new Query(feedUrl);
		myQuery.setStartIndex(1);
		myQuery.setMaxResults(25);
		Feed resultFeed = feedQuery(myQuery);

		int count = 1;
		int size = resultFeed.getTotalResults();
		do {
			
			myQuery.setStartIndex(count);
			if (count>1) resultFeed = feedQuery(myQuery);

			for (Entry entry : resultFeed.getEntries())
			{
				if (entry.getAuthors().get(0).getUri()!=null) {
					//String profileID = entry.getAuthors().get(0).getUri().replace("http://www.blogger.com/profile/","");
					//if (profileID.matches("\\d+")) {
						setMongoComment(postUri, entry);
					//}
				}
				count++; 
			}

		} while (count < size);

	}
	
	private void setMongoPost(Entry entry) throws Exception {
		
		BasicDBObject doc = new BasicDBObject();

		String postID = entry.getSelfLink().getHref().replace("http://www.blogger.com/feeds/","").replace("posts/default/","");					
		String blogID = postID.substring(0,postID.indexOf("/"));
		postID = postID.replace(blogID+"/","");
		
		doc.put("blogID", blogID);
		doc.put("postID", postID);
		
		if (collPosts.find(doc).count()==0) {
			String authorID = entry.getAuthors().get(0).getUri().replace("http://www.blogger.com/profile/","");
			doc.put("authorID", authorID);
			
			//Date published = new Date(entry.getPublished().getValue());
			//doc.put("published", published);
			
			String title = Normalizer.normalize(entry.getTitle().getPlainText(), Normalizer.Form.NFD);
			title = title.replaceAll("[^\\p{ASCII}]", "");
			doc.put("title", title);
			
			if (entry.getContent()!=null) {
				String content = Normalizer.normalize(((TextContent) entry.getContent()).getContent().getPlainText(), Normalizer.Form.NFD);
				content = content.replaceAll("[^\\p{ASCII}]", "");
				content = MongoIterate.Stem(content, MongoIterate.analyzer, MongoIterate.stopStemmWords);
				doc.put("content", content);
			}

			BasicDBList tags = new BasicDBList();
			for ( Category category : entry.getCategories() ) {
				String text = Normalizer.normalize(category.getTerm(), Normalizer.Form.NFD);
				text = text.replaceAll("[^\\p{ASCII}]", "");
				tags.add(tags.size(), text);
			}
			doc.put("tags", tags);
			doc.put("comments", new BasicDBList());

			collPosts.insert(doc);
			getComments(blogID+"/"+postID);
		}
		
	}
	
	private void setMongoComment(String postUri, Entry entry) {

		BasicDBObject doc = new BasicDBObject();
		
		String blogID = postUri.substring(0,postUri.indexOf("/"));
		postUri = postUri.replace(blogID+"/","");
		
		doc.put("blogID", blogID);		
		doc.put("postID", postUri);
		
		if (collPosts.find(doc).count()!=0) {
			
			BasicDBObject post = (BasicDBObject)collPosts.findOne(doc);
		
			BasicDBList comments = (BasicDBList)post.get("comments");
			
			BasicDBObject comment = new BasicDBObject();
			
			//String commentID = entry.getSelfLink().getHref().replace("http://www.blogger.com/feeds/","").replace("comments/default/","");
			//comment.put("commentID", commentID);
			
			String authorID = entry.getAuthors().get(0).getUri().replace("http://www.blogger.com/profile/","");
			//comment.put("authorID", authorID);

			//try {
			//	myStm.executeUpdate("INSERT INTO author SET profileID = '" + authorID + "'");
			//} catch (Exception e) { }

			//Date published = new Date(entry.getPublished().getValue());
			//comment.put("published", published);
			
			//String content = Normalizer.normalize(((TextContent) entry.getContent()).getContent().getPlainText(), Normalizer.Form.NFD);
			//content = content.replaceAll("[^\\p{ASCII}]", "");
			//comment.put("content", content);
			
			comments.put(comments.size(),authorID);
	
			post.append("comments", comments);
		
			collPosts.save(post);
		}

	}

}
