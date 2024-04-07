package org.java.Entity;

import java.io.Serializable;

public class Player implements Serializable
{
    String roundId;
    String matchId;
    String team;
    String couchTeam;
    String lineup;
    String shirtNumber;
    String playerName;
    String position;
    String event;

    public Player(String roundId, String matchId, String team, String couchTeam, String lineup, String shirtNumber, String playerName, String position, String event) {
        this.roundId = roundId;
        this.matchId = matchId;
        this.team = team;
        this.couchTeam = couchTeam;
        this.lineup = lineup;
        this.shirtNumber = shirtNumber;
        this.playerName = playerName;
        this.position = position;
        this.event = event;
    }

    public String getRoundId() {
        return roundId;
    }

    public void setRoundId(String roundId) {
        this.roundId = roundId;
    }

    public String getMatchId() {
        return matchId;
    }

    public void setMatchId(String matchId) {
        this.matchId = matchId;
    }

    public String getTeam() {
        return team;
    }

    public void setTeam(String team) {
        this.team = team;
    }

    public String getCouchTeam() {
        return couchTeam;
    }

    public void setCouchTeam(String couchTeam) {
        this.couchTeam = couchTeam;
    }

    public String getLineup() {
        return lineup;
    }

    public void setLineup(String lineup) {
        this.lineup = lineup;
    }

    public String getShirtNumber() {
        return shirtNumber;
    }

    public void setShirtNumber(String shirtNumber) {
        this.shirtNumber = shirtNumber;
    }

    public String getPlayerName() {
        return playerName;
    }

    public void setPlayerName(String playerName) {
        this.playerName = playerName;
    }

    public String getPosition() {
        return position;
    }

    public void setPosition(String position) {
        this.position = position;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    @Override
    public String toString() {
        return "Player{" +
                "roundId='" + roundId + '\'' +
                ", matchId='" + matchId + '\'' +
                ", team='" + team + '\'' +
                ", couchTeam='" + couchTeam + '\'' +
                ", lineup='" + lineup + '\'' +
                ", shirtNumber='" + shirtNumber + '\'' +
                ", playerName='" + playerName + '\'' +
                ", position='" + position + '\'' +
                ", event='" + event + '\'' +
                '}';
    }
}
