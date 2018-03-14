package app.musicmatch.component.validationdataimport.exception;

public class InvalidRecordStructureException extends ApplicationException {

    public InvalidRecordStructureException() {
    }

    public InvalidRecordStructureException(String message) {
        super(message);
    }

    public InvalidRecordStructureException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidRecordStructureException(Throwable cause) {
        super(cause);
    }

    public InvalidRecordStructureException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
