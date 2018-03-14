package app.musicmatch.component.itembasedsparkmodule;

import java.io.Serializable;
import java.util.Set;

public class UserPlaylist implements Serializable{
    String user;
    Set<String> songs;

    public UserPlaylist(String user, Set<String> songs) {
        this.user = user;
        this.songs = songs;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public Set<String> getSongs() {
        return songs;
    }

    public void setSongs(Set<String> songs) {
        this.songs = songs;
    }
}
