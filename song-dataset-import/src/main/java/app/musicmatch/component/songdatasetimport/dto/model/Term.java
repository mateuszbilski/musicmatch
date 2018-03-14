package app.musicmatch.component.songdatasetimport.dto.model;

public class Term {
    private String term;
    private Double frequency;
    private Double weight;

    public Term(String term, Double frequency, Double weight) {
        this.term = term;
        this.frequency = frequency;
        this.weight = weight;
    }

    public String getTerm() {
        return term;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public Double getFrequency() {
        return frequency;
    }

    public void setFrequency(Double frequency) {
        this.frequency = frequency;
    }

    public Double getWeight() {
        return weight;
    }

    public void setWeight(Double weight) {
        this.weight = weight;
    }
}
