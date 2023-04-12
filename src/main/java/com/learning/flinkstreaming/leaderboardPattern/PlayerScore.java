package com.learning.flinkstreaming.leaderboardPattern;

public class PlayerScore {

    String player;
    Long score;

    @Override
    public String toString() {
        return "PlayerScore{" +
                "player='" + player + '\'' +
                ", score=" + score +
                '}';
    }


    public String getPlayer() {
        return player;
    }

    public void setPlayer(String player) {
        this.player = player;
    }

    public Long getScore() {
        return score;
    }

    public void setScore(Long score) {
        this.score = score;
    }


}
