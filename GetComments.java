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
 
public class GetComments {
 
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

		BlockingQueue<String> queue = new ArrayBlockingQueue<String>(numCrawler*4);

		CrawlerC[] crawler = new CrawlerC[numCrawler];
		for (int i=0; i<crawler.length; i++) {
			crawler[i] = new CrawlerC(queue);
			crawler[i].start();
		}

		ResultSet rs = null;
		int offset = 1;
		while(true)
		{
			offset += 100;
			myStm.executeQuery("SELECT blogID from blogs where country = 'BR' LIMIT "+offset+",100");
			System.out.println("\n---"+offset+"---");

			rs = myStm.getResultSet();
			try {
			    if (!rs.first()) break;
			    if (false) break;
			    while (rs.next()) {
				//System.out.println(rs.getString("blogID"));
                        	if (!queue.offer(rs.getString("blogID"),60,TimeUnit.SECONDS)) {
                               		System.out.println("Offer.Timeout");
                        	}
			    }
			} catch (Exception e) {}

		}

		queue.clear();
	    for (int i=0; i<crawler.length; i++)
	        queue.put(CrawlerC.NO_MORE_WORK);		
		for (int i=0; i<crawler.length; i++)
			crawler[i].join();
		
	}

}

class CrawlerC extends Thread {

	private BloggerService myService;
	private int r;
	private String blog;
	
	public static Connection mysqlConn;
	public static Statement myStm;

	static final String NO_MORE_WORK = new String();

	BlockingQueue<String> q;

    CrawlerC(BlockingQueue<String> q) {
    	this.q = q;
		try {
			Random generator = new Random();
			r = generator.nextInt(100);

			myService = new BloggerService("Mongo-BlogFeed-"+r);
			//myService.setReadTimeout(3000);
	
			mysqlConn = DriverManager.getConnection(GetComments.myConnString);
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
				String blogID  = q.poll(60,TimeUnit.SECONDS);
                        	if (blogID == null) { 
		                     	System.out.println("Poll.Timeout");
					continue;
                        	}	

				//System.out.println(r+": Take(get) : "+blogID);

                if (blogID == NO_MORE_WORK) { break;  }

                URL feedUrl = new URL("http://www.blogger.com/feeds/" + blogID + "/comments/default");
                Query myQuery = new Query(feedUrl);
                myQuery.setMaxResults(25);

		System.out.print(r+"+,");
		Feed resultFeed = myService.query(myQuery, Feed.class);
		
		for (Entry entry : resultFeed.getEntries()) {
		    if (entry.getAuthors().get(0).getUri()!=null) {
			String profileID = entry.getAuthors().get(0).getUri().replaceAll("[^\\d]","");
			if (profileID.length() == 20) {
				try {
				myStm.executeUpdate("INSERT IGNORE INTO author SET profileID = '" + profileID + "'");
				//System.out.print(r+"+,");
				} catch (Exception e) { }
			}
		    }
		}

		} catch (Exception e) { System.out.print(r+"ERR,"); }  

	}

		System.out.println("Bye("+r+")");
		try { myStm.close(); } catch (Exception e) {}
    }

	

}
