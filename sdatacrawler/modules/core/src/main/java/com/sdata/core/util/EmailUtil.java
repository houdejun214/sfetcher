package com.sdata.core.util;

import java.util.Properties;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.Address;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import com.sdata.core.CrawlAppContext;
 
/**
 * @author zhufb
 *
 */
public class EmailUtil {
	
	public static void send(String[] recipeintEmail,String subject, String messageText) {
//		send(recipeintEmail,subject,messageText,new String[]{});
	}
	
    public static void send(String[] recipeintEmail,String subject, String messageText,  String [] attachments) {
		try {
			String senderEmail = "nextsearchcentre@gmail.com";
			String senderMailPassword = "NextSearchCentre";
			String gmail = "smtp.gmail.com";
			Properties props = System.getProperties();
			props.put("mail.smtp.user", senderEmail);
			props.put("mail.smtp.host", "smtp.gmail.com");
			props.put("mail.smtp.port", "465");
			props.put("mail.smtp.starttls.enable", "true");
			props.put("mail.smtp.debug", "true");
			props.put("mail.smtp.auth", "true");
			props.put("mail.smtp.socketFactory.port", "465");
			props.put("mail.smtp.socketFactory.class",
					"javax.net.ssl.SSLSocketFactory");
			props.put("mail.smtp.socketFactory.fallback", "false");
			// Required to avoid security exception.
			MyAuthenticator authentication = new MyAuthenticator(senderEmail,
					senderMailPassword);
			Session session = Session.getDefaultInstance(props, authentication);
			session.setDebug(false);
			MimeMessage message = new MimeMessage(session);
			BodyPart messageBodyPart = new MimeBodyPart();
			messageBodyPart.setText(messageText);
			// Add message text
			Multipart multipart = new MimeMultipart();
			multipart.addBodyPart(messageBodyPart);
			// Attachments should reside in your server.
			// Example "c:\file.txt" or "/home/user/photo.jpg"
			for (int i = 0; i < attachments.length; i++) {
				messageBodyPart = new MimeBodyPart();
				DataSource source = new FileDataSource(attachments[i]);
				messageBodyPart.setDataHandler(new DataHandler(source));
				messageBodyPart.setFileName(attachments[i]);
				multipart.addBodyPart(messageBodyPart);
			}
			message.setContent(multipart);
			message.setSubject(subject);
			message.setFrom(new InternetAddress(senderEmail));
			Address[] array = new Address[recipeintEmail.length];
			for(int i=0;i<recipeintEmail.length;i++){
				array[i] = new InternetAddress(recipeintEmail[i]);
			}
			message.addRecipients(Message.RecipientType.TO, array);
			Transport transport = session.getTransport("smtps");
			transport.connect(gmail, 465, senderEmail, senderMailPassword);
			transport.sendMessage(message, message.getAllRecipients());
			transport.close();
		} catch (MessagingException e) {
			throw new RuntimeException(e);
		}
	}
    
    public static void send(String recipeintEmail,String subject, String messageText) {
    	String[] emails = new String[] {"qiumm820@gmail.com"};
    	send(emails, subject, messageText,new String[]{});
    }
    
    public static void send(String subject, String messageText) {
    	String[] emails = getDefaultEmails();
    	send(emails, subject, messageText,new String[]{});
    }
    
    private static String[] getDefaultEmails(){
		String string = CrawlAppContext.conf.get("emails");
		String[] split = string.split(",");
		return split;
	}
    
    private static class MyAuthenticator extends javax.mail.Authenticator {
        String User;
        String Password;
        public MyAuthenticator (String user, String password) {
            User = user;
            Password = password;
        }
         
        @Override
        public PasswordAuthentication getPasswordAuthentication() {
            return new javax.mail.PasswordAuthentication(User, Password);
        }
    }
     
}
   