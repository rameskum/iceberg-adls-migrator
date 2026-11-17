package com.tds.pde.migration.state;

/**
 * The state store could not be read or written, even after retries. Progress can no longer be tracked, so the
 * run must stop; published files remain recognisable by their blob metadata on resume.
 */
public class StateStoreException extends RuntimeException {

    public StateStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
