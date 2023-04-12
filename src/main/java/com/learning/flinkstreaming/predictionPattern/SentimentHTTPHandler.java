package com.learning.flinkstreaming.predictionPattern;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import org.apache.commons.io.IOUtils;
import java.net.HttpURLConnection;

import java.io.IOException;
import java.util.Properties;
import java.util.logging.Logger;

/*This class finds the sentiment of a given text using the
StanfordNLP Library
 */
public class SentimentHTTPHandler implements HttpHandler {

    private static final Logger LOGGER = Logger.getLogger( SentimentHTTPHandler.class.getName() );

    //Handle an incoming request
    public void handle(HttpExchange exchange)
                            throws IOException {


        String inputStr= IOUtils.toString(exchange.getRequestBody());
        LOGGER.info("Received input "+  inputStr);

        //Get sentiment
        String sentiment = getSentiment(inputStr);

        //Write response to output
        exchange.getResponseHeaders().add("Content-Type", "text/html");
        exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK,
                sentiment.length());
        IOUtils.write(sentiment, exchange.getResponseBody());
        LOGGER.info("Returning Sentiment " + sentiment);
        exchange.close();
    }

    public String getSentiment(String text) {

        String[] sentiments = {"Very Negative", "Negative",
                            "Neutral", "Positive", "Very Positive"};

        Properties props = new Properties();
        //Neutral by default
        int prediction_class = 2;

        LOGGER.info("Doing Sentiment Analysis");

        //Setup sentiment  pipeline
        props.setProperty("annotators",
                            "tokenize, ssplit, pos, parse, sentiment");

        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

        //Find sentiment
        Annotation annotation = pipeline.process(text);

        LOGGER.info("Pipeline processed");

        //Extract sentiment
        for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
            Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
             prediction_class = RNNCoreAnnotations.getPredictedClass(tree);
        }
        LOGGER.info("Got sentiment : " + prediction_class);
        return sentiments[prediction_class];
    }

    public static void main(String[] args) {

        SentimentHTTPHandler thh = new SentimentHTTPHandler();
        LOGGER.info(thh.getSentiment("Hello how are you doing"));
    }
}
