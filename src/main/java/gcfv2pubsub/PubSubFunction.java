package gcfv2pubsub;

import com.google.cloud.functions.CloudEventsFunction;
import com.google.events.cloud.pubsub.v1.MessagePublishedData;
import com.google.events.cloud.pubsub.v1.Message;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cloudevents.CloudEvent;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.sql.*;
import java.util.Base64;
import java.util.Date;
import java.util.Objects;
import java.util.UUID;
import java.util.logging.Logger;

public class PubSubFunction implements CloudEventsFunction {
  private static final Logger logger = Logger.getLogger(PubSubFunction.class.getName());

  private static Timestamp expirationTime;
  public void accept(CloudEvent event) throws  IOException {
    // Get cloud event data as JSON string
    String cloudEventData = new String(Objects.requireNonNull(event.getData()).toBytes());
    // Decode JSON event data to the Pub/Sub MessagePublishedData type
    Gson gson = new Gson();
    MessagePublishedData data = gson.fromJson(cloudEventData, MessagePublishedData.class);
    // Get the message from the data
    Message message = data.getMessage();

    // Get the base64-encoded data from the message & decode it
    String encodedData = message.getData();
    String decodedData = new String(Base64.getDecoder().decode(encodedData));

    // Parse the decoded JSON string to extract the email
    JsonObject jsonObject = gson.fromJson(decodedData, JsonObject.class);
    String email = jsonObject.get("userName").getAsString();
    String firstName = jsonObject.get("first_name").getAsString();
    String lastName = jsonObject.get("last_name").getAsString();
    logger.info("email:"+email);
    sendSimpleMessage(email,firstName,lastName);
    // Log the message
    logger.info("Pub/Sub testing message: " + decodedData);
  }

  private String generateVerificationToken(String email) {
    // Encode email and current timestamp into a token
    long currentTimeMillis = new Date().getTime();
    expirationTime = new Timestamp(currentTimeMillis + (2 * 60 * 1000));
    String token = email + ":" + currentTimeMillis;
    return Base64.getEncoder().encodeToString(token.getBytes());
  }

  public void sendSimpleMessage(String email, String firstName, String lastName) throws IOException {
    String endpoint = "https://api.mailgun.net/v3/cloudnish.me/messages";
    String VerificationToken=generateVerificationToken(email);

    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//    String API_KEY = "8681e2ca40b80860ade66544d58da93e-309b0ef4-7dab6103";
    String API_KEY=System.getenv("API_KEY");
    credentialsProvider.setCredentials(
            new AuthScope("api.mailgun.net", 443),
            new UsernamePasswordCredentials("api", API_KEY));

    // Create HTTP client with credentials
    CloseableHttpClient httpClient = HttpClients.custom()
            .setDefaultCredentialsProvider(credentialsProvider)
            .build();

    // Create POST request
    HttpPost httpPost = new HttpPost(endpoint);

    String verificationLink="https://cloudnish.me/v1/user/authenticate?verificationToken="+VerificationToken;
    String htmlContent = "<html>"
            + "<body>"
            + "<h1>Welcome to Cloud Nish, "+firstName+" "+lastName+" !</h1>"
            + "<p>Thank you for signing up. Please click the following link to verify your email:</p>"
            + "<a href=\"" + verificationLink + "\">Verify Email</a>"
            + "<p>If you are unable to click the link, you can copy and paste it into your browser's address bar.</p>"
            + "<p>We're excited to have you on board!</p>"
            + "<h3>Thanks<h3>"
            + "<p>Cloud Nish team</p>"
            + "</body>"
            + "</html>";
    // Set request body
    String requestBody = "from=Cloudnish Support <support@cloudnish.me>"
            + "&to="+email
            + "&subject=Email verification"
            + "&html="+htmlContent;
    httpPost.setEntity(new StringEntity(requestBody, ContentType.APPLICATION_FORM_URLENCODED));

    // Execute request
    HttpResponse response = httpClient.execute(httpPost);
    // Print response
    HttpEntity entity = response.getEntity();
    if (entity != null) {
      logger.info("Mailgun response:"+EntityUtils.toString(entity));
      if(response.getStatusLine().getStatusCode()==200){

        insertEmailLog(email,true,verificationLink);
        logger.info("Mail Successfully sent to "+email);
      }else if(response.getStatusLine().getStatusCode()==401){
        insertEmailLog(email,false,verificationLink);
        logger.info("Mail Not sent: Authorization error");
      }
    }

    // Close HttpClient
    httpClient.close();
  }

  public void insertEmailLog(String userEmail, boolean emailDeliveryFlag,String verificationLink) {

    String username = System.getenv("DB_USERNAME");
    String password = System.getenv("DB_PASSWORD");
    String databaseIPAddress = System.getenv("DB_IP_Address");
    String url = "jdbc:postgresql://"+databaseIPAddress+":5432/webapp";

    if (username == null || password == null) {
      throw new IllegalArgumentException("Database environment variables not set");
    }
    UUID uuid = UUID.randomUUID();


    Timestamp timestamp = new Timestamp(System.currentTimeMillis());


    String sql = "INSERT INTO email_log (id,user_email, is_email_sent,time_stamp,verification_link,expiration_time) VALUES (?, ?, ?,?,?,?)";

    try (Connection conn = DriverManager.getConnection(url , username, password);
         PreparedStatement pstmt = conn.prepareStatement(sql)) {

      pstmt.setString(1, uuid.toString());
      pstmt.setString(2, userEmail);
      pstmt.setBoolean(3, emailDeliveryFlag);
      pstmt.setTimestamp(4, timestamp);
      pstmt.setString(5, verificationLink);
      pstmt.setTimestamp(6, expirationTime);

      int rowsEffected=pstmt.executeUpdate();
      logger.info("Rows Effected:"+rowsEffected);

      String selectSql = "SELECT * FROM email_log";
      try (PreparedStatement selectPstmt = conn.prepareStatement(selectSql)) {
        // Execute the SELECT query
        ResultSet resultSet = selectPstmt.executeQuery();

        // Process the result set
        while (resultSet.next()) {
          // Assuming your_table has columns "uuid", "user_email", "sent"
          String resultUuid = resultSet.getString("id");
          String resultUserEmail = resultSet.getString("user_email");
          boolean resultSent = resultSet.getBoolean("is_email_sent");
          Timestamp resultTimeStamp= resultSet.getTimestamp("time_stamp");
          Timestamp expirationTimeStamp= resultSet.getTimestamp("expiration_time");
          String verification = resultSet.getString("verification_link");


          logger.info("UUID: " + resultUuid + ", User Email: " + resultUserEmail + ", Sent: " + resultSent+", Time stamp: "+resultTimeStamp+", Expiration Time stamp: "+expirationTimeStamp+", Verification Link: "+verification);
        }
      }
    } catch (SQLException e) {
      e.printStackTrace(); // Handle or log the exception properly
    }
  }

}
