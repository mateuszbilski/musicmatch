package app.musicmatch.component.alssparkmodule.model;

import java.io.Serializable;

public class SongPlayCount implements Serializable {

    private String songId;
    private Integer playCount;

    public SongPlayCount() {

    }

    public SongPlayCount(String songId, Integer playCount) {
        this.songId = songId;
        this.playCount = playCount;
    }

    public String getSongId() {
        return songId;
    }

    public void setSongId(String songId) {
        this.songId = songId;
    }

    public Integer getPlayCount() {
        return playCount;
    }

    public void setPlayCount(Integer playCount) {
        this.playCount = playCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SongPlayCount that = (SongPlayCount) o;

        return songId.equals(that.songId);

    }

    @Override
    public int hashCode() {
        return songId.hashCode();
    }
}
