package app.musicmatch.component.alssparkmodule.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Set;

public class User implements Serializable {

    @JsonProperty("_id")
    private String id;
    private Set<SongPlayCount> songPlayCounts;

    public User() {

    }

    public User(String id, Set<SongPlayCount> songPlayCounts) {
        this.id = id;
        this.songPlayCounts = songPlayCounts;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Set<SongPlayCount> getSongPlayCounts() {
        return songPlayCounts;
    }

    public void setSongPlayCounts(Set<SongPlayCount> songPlayCounts) {
        this.songPlayCounts = songPlayCounts;
    }
}
