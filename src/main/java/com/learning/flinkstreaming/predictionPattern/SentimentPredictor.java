package com.learning.flinkstreaming.predictionPattern;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

/** This is a client class that will call the
 * sentiment analysis HTTP Service and returns the results
 */
public final class SentimentPredictor {

    public static void main(String[] args) {
        System.out.println(SentimentPredictor
                .getSentiment("how are you"));
    }

    //Call Sentiment Service and return results
    public static String getSentiment(String review) {

        CloseableHttpClient httpClient = HttpClients.createDefault();

        try {
            HttpPost request =
                    new HttpPost("http://localhost:8001/sentiment");
            request.setEntity(new StringEntity(review));
            CloseableHttpResponse response = httpClient.execute(request);

            return EntityUtils.toString(response.getEntity());
        }
        catch(Exception e) {
            e.printStackTrace();
            return "Error";
        }
    }
}
