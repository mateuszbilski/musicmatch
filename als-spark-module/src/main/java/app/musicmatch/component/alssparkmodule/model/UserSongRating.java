package app.musicmatch.component.alssparkmodule.model;

import java.io.Serializable;

public class UserSongRating implements Serializable{

    private String userId;
    private String songId;
    private float rating;

    public UserSongRating(String userId, String songId, float rating) {
        this.userId = userId;
        this.songId = songId;
        this.rating = rating;
    }

    public String getUserId() {
        return userId;
    }

    public String getSongId() {
        return songId;
    }

    public float getRating() {
        return rating;
    }
}
