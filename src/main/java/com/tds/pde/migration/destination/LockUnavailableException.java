package com.tds.pde.migration.destination;

public class LockUnavailableException extends RuntimeException {

    public LockUnavailableException(String message) {
        super(message);
    }

    public LockUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
