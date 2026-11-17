package com.tds.pde.migration.dataset;

public class DatasetLookupException extends RuntimeException {

    public DatasetLookupException(String message) {
        super(message);
    }

    public DatasetLookupException(String message, Throwable cause) {
        super(message, cause);
    }
}
