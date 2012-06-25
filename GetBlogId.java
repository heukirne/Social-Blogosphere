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
	public static final int numCrawler = 2;
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
			myStm.executeQuery("SELECT CONCAT_WS('',profileID,'£',blogroll,',',blogs) as info FROM author WHERE retrieve < 10 and find = 5 LIMIT 10");

			rs = myStm.getResultSet();
			try {
			if (!rs.first()) Thread.sleep(60000);
			if (false) break;
			while (rs.next()) {
				//System.out.println(rs.getString("info"));
				blogs = Pattern.compile("£").split(rs.getString("info"));
				//System.out.println("+"+blogs[0]);
				//if (blogs!=null) {
                        		if (!queue.offer(blogs,60,TimeUnit.SECONDS)) {
                                		System.out.println("Offer.Timeout");
                        		}
				//}
			}
			} catch (Exception e) {}

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

			//System.out.println(r+": Start()");
		} catch (Exception e) {
			System.out.println(r+"bye:" + e.getMessage());
		}
		   
    }
    public void run() {
    	while (true) {

	    	try { 
				//System.out.println(r+": Take(wait)");
				//String[] info = q.take();
				String[] info = q.poll(60,TimeUnit.SECONDS);
                        	if (info == null) {
                                	System.out.println("Poll.Timeout");
					continue;
                        	}	

				//System.out.println(r+": Take(get)");
				String[] blogs = null;
				String profileID = "";

                if (info == NO_MORE_WORK) {
                    break;
                }

				if (info.length == 2) {
					profileID = info[0];
					blogs = Pattern.compile(",").split(info[1]);
				} else continue;

				Boolean bSet = false;
	myStm.executeUpdate("UPDATE author SET retrieve = 10 WHERE profileID = '" + profileID + "' LIMIT 1");
	//System.out.println(r+": "+profileID+" - "+ blogs.toString());
	    		for (String blog : blogs)
				{
		if (blog.indexOf("http:")==-1) { System.out.println(r+": continue - "+blog);  continue; }
		String blogID = blog.trim().replace("http:","").replace("/","");

                URL feedUrl = new URL("http://www.blogger.com/feeds/" + blogID + "/comments/default");
                if (!blogID.matches("\\d+")) {
                        feedUrl = new URL("http://" + blogID + "/feeds/comments/default");
                }

                Query myQuery = new Query(feedUrl);
                myQuery.setMaxResults(1);

		//System.out.println(r+": Feed()");
		Feed resultFeed = myService.query(myQuery, Feed.class);
                Matcher matcher = Pattern.compile("\\d+").matcher(resultFeed.getSelfLink().getHref());
                if (matcher.find()) {
			blogID = matcher.group(); 
			System.out.println(r+": "+blogID);
			try {
				myStm.executeUpdate("INSERT INTO blogs SET blogID = '" + blogID + "', totalComments = "+resultFeed.getTotalResults()+" ON DUPLICATE KEY UPDATE totalComments = "+resultFeed.getTotalResults());
			} catch (Exception e) { }

		}

		

				}

			} catch (Exception e) {
				System.out.println(r+"runEx:" + e.getMessage() + blog);
			}

		}

		System.out.println("Bye("+r+")");
		try { myStm.close(); } catch (Exception e) {}
    }

	

}
