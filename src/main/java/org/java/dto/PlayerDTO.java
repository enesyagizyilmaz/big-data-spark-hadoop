package org.java.dto;

import java.io.Serializable;

public class PlayerDTO implements Serializable
{
    String playerName;
    int matchCount;

    public PlayerDTO(String playerName, int matchCount) {
        this.playerName = playerName;
        this.matchCount = matchCount;
    }

    public String getPlayerName() {
        return playerName;
    }

    public void setPlayerName(String playerName) {
        this.playerName = playerName;
    }

    public int getMatchCount() {
        return matchCount;
    }

    public void setMatchCount(int matchCount) {
        this.matchCount = matchCount;
    }
}
