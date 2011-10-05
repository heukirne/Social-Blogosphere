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
 
public class MongoIterate {
 
	public static final String myConnString = "jdbc:mysql://localhost/bloganalysis?user=root&password=";
	public static final int mongoPort = 27017;
	public static final String mongoHost = "localhost";
	public static final int skipInt = 0;
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

		Thread.sleep(500); 

		mongoConn.close();
        myStm.close();
    }
	
	public static void getBlogs() throws Exception 
	{

		final int numCrawler = 1;
		CrawlerM[] crawler = new CrawlerM[numCrawler];

		ResultSet rs = null;
		String[] blogs;
		String profileID = "";
		int contCrawler = 0;

		while(true)
		{
			blogs = null;
			profileID = "";
			//myStm.executeQuery("SELECT blogs, profileID FROM author WHERE Local = 'BR' and length(Blogs)>2 AND Find=1 AND retrieve=0 ORDER BY RAND() DESC LIMIT 1");
			rs = myStm.getResultSet();
			if (rs!=null) {
				rs.next();
				blogs = Pattern.compile(",").split(rs.getString("blogs"));
				profileID = rs.getString("profileID");
			} else {
				blogs = getBlogFromMongo();
				profileID = "0";
			}
		
			if (blogs==null) continue;

			//Better implement a BlockingQueue or Thread Pool Pattern
			crawler[contCrawler] = new CrawlerM(profileID, blogs);
			crawler[contCrawler].start();
			contCrawler++;

			if (contCrawler >= crawler.length) {
				for (int i=0; i<contCrawler; i++)
					crawler[i].join();

				contCrawler = 0;

				String sql = "UPDATE neo4jstats SET " +
							"posts = " + collPosts.getCount() + " LIMIT 1";	
				myStm.executeUpdate(sql);
				if (isExit()) break;
			}

		}
		
	}
	
	private static String[] getBlogFromMongo() {

		DBCollection collBlogs = mongoDb.getCollection("blogCount");

		QueryBuilder query = new QueryBuilder();
		DBObject docUnset = query.start("dot").notEquals(1).and("value").greaterThanEquals(20).get();

		DBCursor cur = collBlogs.find(docUnset).skip(skipInt);
		DBObject obj = null ;
        if(cur.hasNext()) 
         	obj = cur.next();


		String[] blogID = { obj.get("_id").toString() };
		
		return blogID;

	}

	private static Boolean isExit() {
		File file = new File("AuthorIterateExit.txt");
		StringBuffer contents = new StringBuffer();
		BufferedReader reader = null;
		
		 
		try {
			String text = null;
			reader = new BufferedReader(new FileReader(file));
			 
			while ((text = reader.readLine()) != null) {
				contents.append(text);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (reader != null) {
				reader.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		int intExit = Integer.parseInt(contents.toString());
		if (intExit==1)
			return true;
		else
			return false;
	}

}

class CrawlerM extends Thread {

	private String[] blogs;
	private String profileID;


	private Mongo mongoConn;
	private DB mongoDb;
	private DBCollection collPosts;	

    CrawlerM(final String profileID, final String[] blogs) throws Exception {
    	this.profileID = profileID;
        this.blogs = blogs;
    }
    public void run() {
    	try {
		
			mongoConn = new Mongo( MongoIterate.mongoHost , MongoIterate.mongoPort );
			mongoDb = mongoConn.getDB( "blogdb" );
			collPosts = mongoDb.getCollection("posts");
			boolean bOk, bSet = true;

    		for (String blog : blogs)
			{
				String blogID = blog.trim().replace("http:","").replace("/","");
				bOk = getPosts(blogID);
				if (!bOk) bSet = bOk;
				else {
					if (blog.matches("\\d+")) {
						DBCollection collBlogs = mongoDb.getCollection("blogCount");
						BasicDBObject docId = new BasicDBObject();
				        docId.put("_id", blog);

						DBObject obj = collBlogs.findOne(docId);
						obj.put("dot",1);
				        collBlogs.save(obj);
					}
				}
			}

			if (bSet) {
				MongoIterate.myStm.executeUpdate("UPDATE author SET retrieve = 1 WHERE profileID = '" + profileID + "' LIMIT 1");
			}
			mongoConn.close();

		} catch (Exception e) {
			System.out.println("runEx:" + e.getMessage());
		}
		
    }

	private boolean getPosts(final String blogUri) throws ServiceException, IOException {
			
			System.out.print(">>" + blogUri);			
			BloggerService myService = new BloggerService("exampleCo-exampleApp-1");
			
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

			try {
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

				Feed resultFeed = myService.query(myQuery, Feed.class);
				
				String blogID = resultFeed.getSelfLink().getHref().replace("http://www.blogger.com/feeds/","").replace("/posts/default/?published-min=2011-01-01","");					
				
				int count = 1;
				int size = resultFeed.getTotalResults();

				System.out.print("("+size+")");

				do {
					
					myQuery.setStartIndex(count);
					if (count>1) resultFeed = myService.query(myQuery, Feed.class);

					for (Entry entry : resultFeed.getEntries()) {
						String postID = "";
						try {
							postID = entry.getSelfLink().getHref().replace("http://www.blogger.com/feeds/","").replace("posts/default/","");					
						} catch (Exception e) {
							break;
						}
						if (entry.getAuthors().get(0).getUri()!=null) {
							System.out.print("," + count +"{");
							setMongoPost(entry);
							getComments(postID);
							System.out.print("}");
							count++; 
						}
					}

				} while (count < size);
				
				System.out.print("\n");
				return true;
			} catch (MalformedURLException e) {
				System.out.println("Malformed URL Exception!");
				return false;
			} catch (IOException e) {
				System.out.println("IOException!");
				return false;
			} catch (ServiceException e) {
				System.out.println("Service Exception: "+ e.getMessage());
				return false;
			} catch (Exception e) {
				System.out.println("getPostsEx: " + e.getMessage());
				return false;
			}
	}
	
	private void getComments(final String postUri) throws ServiceException, IOException {
			BloggerService myService = new BloggerService("exampleCo-exampleApp-1");

			try {
				URL feedUrl = new URL("http://www.blogger.com/feeds/" + postUri + "/comments/default");
				Query myQuery = new Query(feedUrl);
				myQuery.setStartIndex(1);
				myQuery.setMaxResults(25);
				Feed resultFeed = myService.query(myQuery, Feed.class);

				int count = 1;
				int size = resultFeed.getTotalResults();
				System.out.print(size+":");
				do {
					
					myQuery.setStartIndex(count);
					if (count>1) resultFeed = myService.query(myQuery, Feed.class);

					for (Entry entry : resultFeed.getEntries())
					{
						if (entry.getAuthors().get(0).getUri()!=null) {
							String profileID = entry.getAuthors().get(0).getUri().replace("http://www.blogger.com/profile/","");
							if (profileID.matches("\\d+")) {
								setMongoComment(postUri, entry);
							}
						}
						System.out.print(","+count);
						count++; 
					}

				} while (count < size);

			} catch (MalformedURLException e) {
				System.out.println("Malformed URL Exception!");
			} catch (IOException e) {
				System.out.println("IOException!");
			} catch (ServiceException e) {
				System.out.println("Service Exception: "+ e.getMessage());
			} catch (Exception e) {
				System.out.println("getCommentsEx: " + e.getMessage());
			}
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
				MongoIterate.myStm.executeUpdate("INSERT INTO author SET profileID = '" + authorID + "'");
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