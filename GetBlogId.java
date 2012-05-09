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

public class GetBlogId {
 
	public static String myConnString = "jdbc:mysql://localhost/bloganalysis?user=&password=";
	public static final int numCrawler = 10;
	public static Connection mysqlConn;
	public static Statement myStm;

    public static void main(String[] args) throws Exception {		

Properties configFile = new Properties();
configFile.load( new FileInputStream("my_config.properties"));
myConnString = configFile.getProperty("MYCONN");

		try {
			mysqlConn = DriverManager.getConnection(myConnString);
			myStm = mysqlConn.createStatement();
			myStm.executeQuery("set wait_timeout = 7200");
		} catch (Exception e) {
			System.out.println("MySQL Offline.");
			System.exit(1);
		}
		
		getBlogs();

		Thread.sleep(1000); //For cleaning mongo cursos 

        	myStm.close();
    }
	
	public static void getBlogs() throws Exception 
	{

		BlockingQueue<String[]> queue = new ArrayBlockingQueue<String[]>(numCrawler*4);

		CrawlerG[] crawler = new CrawlerG[numCrawler];
		for (int i=0; i<crawler.length; i++) {
			crawler[i] = new CrawlerG(queue);
			crawler[i].start();
		}

		ResultSet rs = null;
		String[] blogs;

		while(true)
		{
			blogs = null;
			myStm.executeQuery("SELECT CONCAT(profileID, '#' , blogs) as info FROM author WHERE length(Blogs)>2 and retrieve=0 ORDER BY RAND() DESC LIMIT 1");
			rs = myStm.getResultSet();
			try {
				if (rs.first()) {
					blogs = Pattern.compile("#").split(rs.getString("info"));
				}
			} catch (Exception e) {}

			if (blogs==null) break;

			queue.put(blogs);

		}

		queue.clear();
	    for (int i=0; i<crawler.length; i++)
	        queue.put(CrawlerG.NO_MORE_WORK);		
		for (int i=0; i<crawler.length; i++)
			crawler[i].join();
		
	}

}

class CrawlerG extends Thread {

	private BloggerService myService;
	private int r;
	private String blog;
	
	public static Connection mysqlConn;
	public static Statement myStm;

	static final String[] NO_MORE_WORK = new String[]{};

	BlockingQueue<String[]> q;

    CrawlerG(BlockingQueue<String[]> q) {
    	this.q = q;
		try {
			Random generator = new Random();
			r = generator.nextInt(100);

			myService = new BloggerService("Mongo-BlogFeed-"+r);
			//myService.setReadTimeout(3000);

			mysqlConn = DriverManager.getConnection(GetBlogId.myConnString);
			myStm = mysqlConn.createStatement();
			myStm.executeQuery("set wait_timeout = 7200");
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

				Boolean bSet = false;
	myStm.executeUpdate("UPDATE author SET retrieve = 9 WHERE profileID = '" + profileID + "' LIMIT 1");
	    		for (String blogFind : blogs)
				{
					blog = blogFind;
					String blogID = blog.trim().replace("http:","").replace("/","");

                URL feedUrl = new URL("http://www.blogger.com/feeds/" + blogID + "/posts/default");
                if (!blogID.matches("\\d+")) {
                        feedUrl = new URL("http://" + blogID + "/feeds/posts/default");
                }

                Query myQuery = new Query(feedUrl);
                myQuery.setMaxResults(1);

		Feed resultFeed = myService.query(myQuery, Feed.class);
                Matcher matcher = Pattern.compile("\\d+").matcher(resultFeed.getSelfLink().getHref());
                if (matcher.find()) {
			blogID = matcher.group(); 
			System.out.println(blogID);
			try {
				myStm.executeUpdate("INSERT INTO blogs SET blogID = '" + blogID + "'");
			} catch (Exception e) { }

		}

		

				}

			} catch (Exception e) {
				System.out.println(r+"runEx:" + e.getMessage());
			}

		}

		System.out.println("Bye("+r+")");
		try { myStm.close(); } catch (Exception e) {}
    }

	

}
