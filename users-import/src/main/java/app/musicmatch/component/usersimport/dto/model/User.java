package app.musicmatch.component.usersimport.dto.model;

import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;

import java.util.List;
import java.util.Set;

@Entity("users")
public class User {

    @Id
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
