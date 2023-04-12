package com.learning.flinkstreaming.predictionPattern;

public class WebsiteReview {
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getReview() {
        return review;
    }

    public void setReview(String review) {
        this.review = review;
    }

    public String getSentiment() {
        return sentiment;
    }

    public void setSentiment(String sentiment) {
        this.sentiment = sentiment;
    }

    @Override
    public String toString() {
        return "WebsiteReview{" +
                "id='" + id + '\'' +
                ", review='" + review + '\'' +
                ", sentiment='" + sentiment + '\'' +
                '}';
    }

    String id;
    String review;
    String sentiment;


}
