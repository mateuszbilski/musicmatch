package app.musicmatch.component.songdatasetimport.exception;

public class InvalidRecordStructure extends ApplicationException {

    public InvalidRecordStructure() {
    }

    public InvalidRecordStructure(String message) {
        super(message);
    }

    public InvalidRecordStructure(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidRecordStructure(Throwable cause) {
        super(cause);
    }

    public InvalidRecordStructure(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
