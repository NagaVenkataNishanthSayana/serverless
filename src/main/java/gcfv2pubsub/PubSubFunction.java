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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Base64;
import java.util.Date;
import java.util.Objects;
import java.util.UUID;
import java.util.logging.Logger;

public class PubSubFunction implements CloudEventsFunction {
  private static final Logger logger = Logger.getLogger(PubSubFunction.class.getName());
  private final String API_KEY="024ce93a526ef27542a01d7018601717";

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
    logger.info("email:"+email);
    sendSimpleMessage(email);
    // Log the message
    logger.info("Pub/Sub testing message: " + decodedData);
  }

  private String generateVerificationToken(String email) {
    // Encode email and current timestamp into a token
    String token = email + ":" + new Date().getTime();
    return Base64.getEncoder().encodeToString(token.getBytes());
  }

  public void sendSimpleMessage(String email) throws IOException {
    String endpoint = "https://api.mailgun.net/v3/cloudnish.me/messages";
    String VerificationToken=generateVerificationToken(email);

    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(
            new AuthScope("api.mailgun.net", 443),
            new UsernamePasswordCredentials("api", API_KEY));

    // Create HTTP client with credentials
    CloseableHttpClient httpClient = HttpClients.custom()
            .setDefaultCredentialsProvider(credentialsProvider)
            .build();

    // Create POST request
    HttpPost httpPost = new HttpPost(endpoint);

    String verificationLink="http://localhost:8080/v1/user/authenticate?verificationToken="+VerificationToken;
    String htmlContent = "<html>"
            + "<body>"
            + "<h1>Welcome to our service!</h1>"
            + "<p>Thank you for signing up. Please click the following link to verify your email:</p>"
            + "<a href=\"" + verificationLink + "\">Verify Email</a>"
            + "<p>If you are unable to click the link, you can copy and paste it into your browser's address bar.</p>"
            + "<p>We're excited to have you on board!</p>"
            + "</body>"
            + "</html>";
    // Set request body
    String requestBody = "from=Cloudnish Admin User <admin@cloudnish.me>"
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

        insertEmailLog(email,true);
        logger.info("Mail Successfully sent to "+email);
      }else if(response.getStatusLine().getStatusCode()==401){
        insertEmailLog(email,false);
        logger.info("Mail Not sent: Authorization error");
      }
    }

    // Close HttpClient
    httpClient.close();
  }

  public void insertEmailLog(String userEmail, boolean sent) {

    String username = System.getenv("DB_USERNAME");
    String password = System.getenv("DB_PASSWORD");
    String databaseIPAddress = System.getenv("DB_IP_Address");
    String url = "jdbc:postgresql://"+databaseIPAddress+":5432/webapp";

    if (username == null || password == null) {
      throw new IllegalArgumentException("Database environment variables not set");
    }
    UUID uuid = UUID.randomUUID();


    String sql = "INSERT INTO email_logs (id,user_email, is_email_sent) VALUES (?, ?, ?)";

    try (Connection conn = DriverManager.getConnection(url , username, password);
         PreparedStatement pstmt = conn.prepareStatement(sql)) {

      pstmt.setString(1, uuid.toString());
      pstmt.setString(2, userEmail);
      pstmt.setBoolean(3, sent);

      pstmt.executeUpdate();
    } catch (SQLException e) {
      e.printStackTrace(); // Handle or log the exception properly
    }
  }

}
