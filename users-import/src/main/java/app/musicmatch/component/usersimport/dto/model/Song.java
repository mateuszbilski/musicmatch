package app.musicmatch.component.usersimport.dto.model;

import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;

import java.util.List;

@Entity("songs")
public class Song {

    @Id
    private String id;
    private Double artistFamiliarity;
    private Double artistHotness;
    private String artistId;
    private String artistName;
    private List<Term> artistTerms;
    private Double danceability;
    private Double duration;
    private Double energy;
    private Integer key;
    private Double keyConfidence;
    private Integer mode;
    private Double modeConfidence;
    private String release;
    //Skipped release_7digitalid
    private List<String> similarArtists;
    private Double songHotness;
    private String songId;
    private Double tempo;
    private Integer timeSignature;
    private Double timeSignatureConfidence;
    private String title;
    private String trackId;
    private Integer year;

    private Song() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Double getArtistFamiliarity() {
        return artistFamiliarity;
    }

    public void setArtistFamiliarity(Double artistFamiliarity) {
        this.artistFamiliarity = artistFamiliarity;
    }

    public Double getArtistHotness() {
        return artistHotness;
    }

    public void setArtistHotness(Double artistHotness) {
        this.artistHotness = artistHotness;
    }

    public String getArtistId() {
        return artistId;
    }

    public void setArtistId(String artistId) {
        this.artistId = artistId;
    }

    public String getArtistName() {
        return artistName;
    }

    public void setArtistName(String artistName) {
        this.artistName = artistName;
    }

    public List<Term> getArtistTerms() {
        return artistTerms;
    }

    public void setArtistTerms(List<Term> artistTerms) {
        this.artistTerms = artistTerms;
    }

    public Double getDanceability() {
        return danceability;
    }

    public void setDanceability(Double danceability) {
        this.danceability = danceability;
    }

    public Double getDuration() {
        return duration;
    }

    public void setDuration(Double duration) {
        this.duration = duration;
    }

    public Double getEnergy() {
        return energy;
    }

    public void setEnergy(Double energy) {
        this.energy = energy;
    }

    public Integer getKey() {
        return key;
    }

    public void setKey(Integer key) {
        this.key = key;
    }

    public Double getKeyConfidence() {
        return keyConfidence;
    }

    public void setKeyConfidence(Double keyConfidence) {
        this.keyConfidence = keyConfidence;
    }

    public Integer getMode() {
        return mode;
    }

    public void setMode(Integer mode) {
        this.mode = mode;
    }

    public Double getModeConfidence() {
        return modeConfidence;
    }

    public void setModeConfidence(Double modeConfidence) {
        this.modeConfidence = modeConfidence;
    }

    public String getRelease() {
        return release;
    }

    public void setRelease(String release) {
        this.release = release;
    }

    public List<String> getSimilarArtists() {
        return similarArtists;
    }

    public void setSimilarArtists(List<String> similarArtists) {
        this.similarArtists = similarArtists;
    }

    public Double getSongHotness() {
        return songHotness;
    }

    public void setSongHotness(Double songHotness) {
        this.songHotness = songHotness;
    }

    public String getSongId() {
        return songId;
    }

    public void setSongId(String songId) {
        this.songId = songId;
    }

    public Double getTempo() {
        return tempo;
    }

    public void setTempo(Double tempo) {
        this.tempo = tempo;
    }

    public Integer getTimeSignature() {
        return timeSignature;
    }

    public void setTimeSignature(Integer timeSignature) {
        this.timeSignature = timeSignature;
    }

    public Double getTimeSignatureConfidence() {
        return timeSignatureConfidence;
    }

    public void setTimeSignatureConfidence(Double timeSignatureConfidence) {
        this.timeSignatureConfidence = timeSignatureConfidence;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getTrackId() {
        return trackId;
    }

    public void setTrackId(String trackId) {
        this.trackId = trackId;
    }

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }


    public static final class Builder {
        private String id;
        private Double artistFamiliarity;
        private Double artistHotness;
        private String artistId;
        private String artistName;
        private List<Term> artistTerms;
        private Double danceability;
        private Double duration;
        private Double energy;
        private Integer key;
        private Double keyConfidence;
        private Integer mode;
        private Double modeConfidence;
        private String release;
        //Skipped release_7digitalid
        private List<String> similarArtists;
        private Double songHotness;
        private String songId;
        private Double tempo;
        private Integer timeSignature;
        private Double timeSignatureConfidence;
        private String title;
        private String trackId;
        private Integer year;

        private Builder() {
        }

        public static Builder aSong() {
            return new Builder();
        }

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public Builder withArtistFamiliarity(Double artistFamiliarity) {
            this.artistFamiliarity = artistFamiliarity;
            return this;
        }

        public Builder withArtistHotness(Double artistHotness) {
            this.artistHotness = artistHotness;
            return this;
        }

        public Builder withArtistId(String artistId) {
            this.artistId = artistId;
            return this;
        }

        public Builder withArtistName(String artistName) {
            this.artistName = artistName;
            return this;
        }

        public Builder withArtistTerms(List<Term> artistTerms) {
            this.artistTerms = artistTerms;
            return this;
        }

        public Builder withDanceability(Double danceability) {
            this.danceability = danceability;
            return this;
        }

        public Builder withDuration(Double duration) {
            this.duration = duration;
            return this;
        }

        public Builder withEnergy(Double energy) {
            this.energy = energy;
            return this;
        }

        public Builder withKey(Integer key) {
            this.key = key;
            return this;
        }

        public Builder withKeyConfidence(Double keyConfidence) {
            this.keyConfidence = keyConfidence;
            return this;
        }

        public Builder withMode(Integer mode) {
            this.mode = mode;
            return this;
        }

        public Builder withModeConfidence(Double modeConfidence) {
            this.modeConfidence = modeConfidence;
            return this;
        }

        public Builder withRelease(String release) {
            this.release = release;
            return this;
        }

        public Builder withSimilarArtists(List<String> similarArtists) {
            this.similarArtists = similarArtists;
            return this;
        }

        public Builder withSongHotness(Double songHotness) {
            this.songHotness = songHotness;
            return this;
        }

        public Builder withSongId(String songId) {
            this.songId = songId;
            return this;
        }

        public Builder withTempo(Double tempo) {
            this.tempo = tempo;
            return this;
        }

        public Builder withTimeSignature(Integer timeSignature) {
            this.timeSignature = timeSignature;
            return this;
        }

        public Builder withTimeSignatureConfidence(Double timeSignatureConfidence) {
            this.timeSignatureConfidence = timeSignatureConfidence;
            return this;
        }

        public Builder withTitle(String title) {
            this.title = title;
            return this;
        }

        public Builder withTrackId(String trackId) {
            this.trackId = trackId;
            return this;
        }

        public Builder withYear(Integer year) {
            this.year = year;
            return this;
        }

        public Song build() {
            Song song = new Song();
            song.setId(id);
            song.setArtistFamiliarity(artistFamiliarity);
            song.setArtistHotness(artistHotness);
            song.setArtistId(artistId);
            song.setArtistName(artistName);
            song.setArtistTerms(artistTerms);
            song.setDanceability(danceability);
            song.setDuration(duration);
            song.setEnergy(energy);
            song.setKey(key);
            song.setKeyConfidence(keyConfidence);
            song.setMode(mode);
            song.setModeConfidence(modeConfidence);
            song.setRelease(release);
            song.setSimilarArtists(similarArtists);
            song.setSongHotness(songHotness);
            song.setSongId(songId);
            song.setTempo(tempo);
            song.setTimeSignature(timeSignature);
            song.setTimeSignatureConfidence(timeSignatureConfidence);
            song.setTitle(title);
            song.setTrackId(trackId);
            song.setYear(year);
            return song;
        }
    }
}
