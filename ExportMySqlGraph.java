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
 
public class ExportMySqlGraph {
  
    public static Connection mysqlConn;
    public static Statement myStm;
    public static String myConnString = "";

	
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


		Writer output = null;
		File file = new File("blogroll.graphml");
		output = new BufferedWriter(new FileWriter(file));  
		
		output.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
		output.write("<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\">\n");
		output.write("<key attr.name=\"network\" attr.type=\"int\" for=\"node\" id=\"network\"/>\n");
		output.write("<graph edgedefault=\"directed\">\n");


  			String[] blogs;
			String[] blogroll;
			ResultSet rs = null;

			myStm.executeQuery("SELECT blogs, blogroll FROM author WHERE length(blogs)>2 and length(blogroll)>2 and local = 'BR';");

                        rs = myStm.getResultSet();
                        try {
                        while (rs.next()) {
                                blogs = Pattern.compile(",").split(rs.getString("blogs"));
				blogroll = Pattern.compile(",").split(rs.getString("blogroll"));
                                if (blogs!=null && blogroll!=null) {
                                	for (String source : blogs) {
						for (String target : blogroll) {
							if (source.indexOf("blogspot") > 1 && target.indexOf("blogspot") > 1 && target.indexOf("&") == -1)
							output.write("<edge source=\"" + source.replace("http://","").replace(".blogspot.com/","").trim()
                                                	+ "\" target=\"" + target.replace("http://","").replace(".blogspot.com/","").trim() + "\" />\n");
						}
					}
				}
                        }
                        } catch (Exception e) {}

		output.write("</graph>\n");
		output.write("</graphml>");
		
		output.close();
		
	System.out.println("blogroll.graphml");

	myStm.close();		
    }

}

