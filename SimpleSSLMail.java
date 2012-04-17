import javax.mail.*;
import javax.mail.internet.*;
import java.util.Properties;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.DriverManager;

public class SimpleSSLMail {

    private static final String SMTP_HOST_NAME = "localhost";
    private static final int SMTP_HOST_PORT = 465;
    private static final String SMTP_AUTH_USER = "hdpsantos";
    private static final String SMTP_AUTH_PWD  = "pass";

    public static final String myConnString = "jdbc:mysql://localhost/bloganalysis?user=profile&password=profile";
    public static Connection mysqlConn;
    public static Statement myStm;

    public static void main(String[] args) throws Exception{
       new SimpleSSLMail().test();
    }

    public void test() throws Exception{
		try {
		mysqlConn = DriverManager.getConnection(myConnString);
		myStm = mysqlConn.createStatement();
		myStm.executeQuery("set wait_timeout = 7200");
		} catch (Exception e) {
			System.out.println("MySQL Offline.");
			System.exit(1);
		}

        Properties props = new Properties();

        props.put("mail.transport.protocol", "smtps");
        props.put("mail.smtps.host", SMTP_HOST_NAME);
        props.put("mail.smtps.auth", "true");
        props.put("mail.smtps.quitwait", "false");

        Session mailSession = Session.getDefaultInstance(props);
        //mailSession.setDebug(true);
        Transport transport = mailSession.getTransport();

        MimeMessage message = new MimeMessage(mailSession);
        message.setSubject("Estudo sobre o Blogspot no Brasil");

String htmlText = "<html>"
+"	<body>"
+"	</body>"
+"</html>";

        message.setContent(htmlText, "text/html");
	message.addFrom(new InternetAddress[] { new InternetAddress("hdpsantos","Dias") });
        transport.connect (SMTP_HOST_NAME, SMTP_HOST_PORT, SMTP_AUTH_USER, SMTP_AUTH_PWD);

		myStm = mysqlConn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                myStm.executeQuery("SELECT * FROM author WHERE news = 9 ORDER BY PopLevel DESC limit 10;");
                ResultSet uprs = myStm.getResultSet();
                while (uprs.next()) {
                        System.out.print(uprs.getString("email")+": ");
        
			message = new MimeMessage(mailSession);
        	message.setSubject("Estudo sobre o Blogspot no Brasil");
			message.setContent(htmlText.replace("[Nome]",uprs.getString("nome")).replace("[Numero]",uprs.getString("profileID")), "text/html");
			message.addFrom(new InternetAddress[] { new InternetAddress("hdpsantos","Dias") });
        	message.setRecipient(Message.RecipientType.TO,new InternetAddress(uprs.getString("email")));
        		
			try {
				transport.sendMessage(message,message.getRecipients(Message.RecipientType.TO));
               	System.out.println("Enviado");
				uprs.updateInt("news",90);
				uprs.updateRow();
				Thread.currentThread().sleep(10000);
			 } catch (Exception ex) { System.out.println(ex.getMessage()); }
		}

        transport.close();
	shutdown();
    }

    private static void shutdown()
    {
		System.out.println( "Shutting down database ..." );
		try {
			myStm.close();
		} catch (Exception ex) {}
    }

}
