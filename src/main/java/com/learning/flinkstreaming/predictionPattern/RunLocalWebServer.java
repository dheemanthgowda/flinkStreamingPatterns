package com.learning.flinkstreaming.predictionPattern;

import com.sun.net.httpserver.HttpServer;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/*** This class starts a local webserver on Port 8001 and provides
 * a sentiment analysis service
 */
public class RunLocalWebServer {

    HttpServer server;

    public static void main(String[] args) {

        try {
            RunLocalWebServer localServer = new RunLocalWebServer();
            localServer.startServer();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void startServer() throws Exception{

        System.out.println("Starting Web Server to server sentiments..");

        server = HttpServer.create(new InetSocketAddress(
                    "localhost", 8001), 0);

        ThreadPoolExecutor threadPoolExecutor
                = (ThreadPoolExecutor) Executors
                        .newFixedThreadPool(10);

        //Sentiment handler uses the SentimentHTTPHandler class
        server.createContext("/sentiment", new SentimentHTTPHandler());
        server.setExecutor(threadPoolExecutor);
        server.start();
        System.out.println("Started Local web server");

    }


}


