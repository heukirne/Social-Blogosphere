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

import java.util.regex.*;
import java.util.*;
import java.net.*;
import java.io.*;
import java.text.Normalizer;
import java.text.SimpleDateFormat;
import java.util.concurrent.*;
 
public class MongoIterate {
 
	public static final String myConnString = "jdbc:mysql://localhost/bloganalysis?user=root&password=";
	public static final int mongoPort = 27017;
	public static final String mongoHost = "localhost";
	public static final int numCrawler = 4;
	public static Mongo mongoConn;
	public static DB mongoDb;
	public static DBCollection collPosts;
	public static Connection mysqlConn;
	public static Statement myStm;
	
    public static void main(String[] args) throws Exception {		

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

		BlockingQueue<String[]> queue = new ArrayBlockingQueue<String[]>(numCrawler*4);

		CrawlerM[] crawler = new CrawlerM[numCrawler];
		for (int i=0; i<crawler.length; i++) {
			crawler[i] = new CrawlerM(queue);
			crawler[i].start();
		}

		ResultSet rs = null;
		String[] blogs;

		while(true)
		{
			blogs = null;
			myStm.executeQuery("SELECT CONCAT(profileID, '#' , blogs) as info FROM author WHERE Local = 'BR' and length(Blogs)>2 AND Find=1 AND retrieve=0 ORDER BY RAND() DESC LIMIT 1");
			rs = myStm.getResultSet();
			try {
				if (true && rs.first()) {
					blogs = Pattern.compile("#").split(rs.getString("info"));
				} else {
					blogs = getBlogFromMongo();
				}
			} catch (Exception e) {}

			if (blogs==null) break;

			queue.put(blogs);

			if (queue.size() >= (numCrawler*2)) {
				myStm.executeUpdate("UPDATE neo4jstats SET posts = " + collPosts.getCount() + " LIMIT 1");
			}

		}

		queue.clear();
	    for (int i=0; i<crawler.length; i++)
	        queue.put(CrawlerM.NO_MORE_WORK);		
		for (int i=0; i<crawler.length; i++)
			crawler[i].join();
		
	}
	
	private static String[] getBlogFromMongo() {

		DBCollection collBlogs = mongoDb.getCollection("blogCount");

		QueryBuilder query = new QueryBuilder();
		DBObject docUnset = query.start("dot").notEquals(1).and("value").greaterThanEquals(20).get();

		Random generator = new Random();
		int r = generator.nextInt(collBlogs.find(docUnset).size());

		DBCursor cur = collBlogs.find(docUnset).skip(r);
		DBObject obj = null ;
        if(cur.hasNext()) 
         	obj = cur.next();
        else
        	return null;

		String[] blogID = { obj.get("_id").toString() };
		
		return blogID;

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

	static final String[] NO_MORE_WORK = new String[]{};

	BlockingQueue<String[]> q;

    CrawlerM(BlockingQueue<String[]> q) {
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
				String[] info = q.take();
				String[] blogs = null;
				String profileID = "";

                if (info == NO_MORE_WORK) {
                    break;
                }

				if (info.length == 2) {
					profileID = info[0];
					blogs = Pattern.compile(",").split(info[1]);
				} else {
					blogs = info;
				}

	    		for (String blogFind : blogs)
				{
					blog = blogFind;
					String blogID = blog.trim().replace("http:","").replace("/","");
					
					myStm.executeUpdate("UPDATE author SET retrieve = 1 WHERE profileID = '" + profileID + "' LIMIT 1");
					if (blog.matches("\\d+")) {
						DBCollection collBlogs = mongoDb.getCollection("blogCount");
						BasicDBObject docId = new BasicDBObject();
				        docId.put("_id", blog);

						DBObject obj = collBlogs.findOne(docId);
						obj.put("dot",1);
				        collBlogs.save(obj);
					}

					getPosts(blogID);
				}

				System.out.println("Finish("+r+")");
			} catch (Exception e) {
				System.out.println(r+"runEx:" + e.getMessage());
				e.printStackTrace();
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

	private boolean getPosts(final String blogUri) throws Exception {			
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		
		URL feedUrl = new URL("http://www.blogger.com/feeds/" + blogUri + "/posts/default");
		if (!blogUri.matches("\\d+")) {
			feedUrl = new URL("http://" + blogUri + "/feeds/posts/default");
		} 

		Query myQuery = new Query(feedUrl);
		DateTime dtMin = DateTime.parseDate("2011-01-01");
		myQuery.setPublishedMin(dtMin);	

		if (blogUri.matches("\\d+")) {
			BasicDBObject doc = new BasicDBObject();
			doc.put("blogID", blogUri);
			BasicDBObject sortDoc = new BasicDBObject();
	        sortDoc.put("published", -1);

	        if (collPosts.find(doc).size() > 0) {
				DBCursor cur = collPosts.find(doc).sort(sortDoc);
		        if(cur.hasNext()) {
		        	Date dateChange = formatter.parse("2011-10-03"); //Change Crawler Date
		        	DBObject obj = cur.next();
		        	dtMin = new DateTime((Date)obj.get("published"));
					if (collPosts.find(doc).size() > 30 || dateChange.compareTo((Date)obj.get("published")) < 0) 
					    myQuery.setPublishedMin(dtMin);
		        }
	        }

		}

		Feed resultFeed = feedQuery(myQuery);
		
		int count = 1;
		int size = resultFeed.getTotalResults();

		do {
			if (size<1) break;
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
					getComments(postID);
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
					
					Matcher matcherAuthor = Pattern.compile("\\d+").matcher(entry.getAuthors().get(0).getUri());
					if (matcherAuthor.find()) setMongoComment(postUri, entry);

				}
				count++; 
			}

		} while (count < size);

	}
	
	private void setMongoPost(Entry entry) {
		
		BasicDBObject doc = new BasicDBObject();

		String postID = entry.getSelfLink().getHref().replace("http://www.blogger.com/feeds/","").replace("posts/default/","");					
		String blogID = postID.substring(0,postID.indexOf("/"));
		postID = postID.replace(blogID+"/","");
		
		doc.put("blogID", blogID);
		doc.put("postID", postID);
		
		if (collPosts.find(doc).count()==0) {
			String authorID = entry.getAuthors().get(0).getUri().replace("http://www.blogger.com/profile/","");
			doc.put("authorID", authorID);
			
			Date published = new Date(entry.getPublished().getValue());
			doc.put("published", published);
			
			String title = Normalizer.normalize(entry.getTitle().getPlainText(), Normalizer.Form.NFD);
			title = title.replaceAll("[^\\p{ASCII}]", "");
			doc.put("title", title);
			
			if (entry.getContent()!=null) {
				String content = Normalizer.normalize(((TextContent) entry.getContent()).getContent().getPlainText(), Normalizer.Form.NFD);
				content = content.replaceAll("[^\\p{ASCII}]", "");
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
			
			String commentID = entry.getSelfLink().getHref().replace("http://www.blogger.com/feeds/","").replace("comments/default/","");
			comment.put("commentID", commentID);
			
			String authorID = entry.getAuthors().get(0).getUri().replace("http://www.blogger.com/profile/","");
			comment.put("authorID", authorID);

			try {
				myStm.executeUpdate("INSERT INTO author SET profileID = '" + authorID + "'");
			} catch (Exception e) { }

			Date published = new Date(entry.getPublished().getValue());
			comment.put("published", published);
			
			String content = Normalizer.normalize(((TextContent) entry.getContent()).getContent().getPlainText(), Normalizer.Form.NFD);
			content = content.replaceAll("[^\\p{ASCII}]", "");
			comment.put("content", content);
			
			comments.put(comments.size(),comment);
	
			post.append("comments", comments);
		
			collPosts.save(post);
		}
	
	}

}