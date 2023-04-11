package com.learning.flinkstreaming.chapter6;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.Iterator;
import java.util.Set;

public class RedisManager implements Runnable {

    /****************************************************************************
     * This Class prints leaderboards from redis server running
     * on localhost:6379.
     ***************************************************************************
     **/

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_BLUE = "\u001B[34m";

    private static String lbKey = "topics-leaderboard";

    private Jedis jedis;

    public static void main(String[] args) {

        RedisManager rmgr = new RedisManager();
        rmgr.setUp();
        Thread testThread = new Thread(rmgr);
        testThread.start();

        //Testing the leaderboard.
        //Redis connections are not threadsafe.
        //Open new connection for writing.
        Jedis jedisWriter = new Jedis("localhost");
        try {
            jedisWriter.zincrby(lbKey,2,"AI");
            jedisWriter.zincrby(lbKey,3,"Big Data");
            Thread.currentThread().sleep(6000);
            jedisWriter.zincrby(lbKey,1,"Cloud");
            jedisWriter.zincrby(lbKey,2,"AI");
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    //Create a connection and reset the leaderboard
    public void setUp() {
        try{
            //Jedis running on localhost and port 6379
            jedis =new Jedis("localhost");
            //reset the key
            jedis.del(lbKey);
            System.out.println("Redis connection setup successfully");
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    public void update_score(String product, double count) {
        jedis.zincrby(lbKey,count,product);
    }

    public void run() {

        try {
            while (true) {

                //Query the leaderboard and print the results
                Set<Tuple> scores=
                        jedis.zrevrangeWithScores(
                                lbKey,0,-1);

                Iterator<Tuple> iScores = scores.iterator();
                int position=1;

                while (iScores.hasNext()) {
                    Tuple score= iScores.next();
                    System.out.println(
                            ANSI_BLUE + "Trending Topics - " + position + " : "
                            +  score.getElement() + " = " + score.getScore()
                            + ANSI_RESET);
                    position++;
                }

                Thread.currentThread().sleep(5000);
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }

}
