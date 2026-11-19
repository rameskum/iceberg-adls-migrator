package io.delta.exceptions;

import java.util.ConcurrentModificationException;

/**
 * Test stand-in for one of Delta's commit conflict exceptions, which extend {@link ConcurrentModificationException}
 * and carry a SQLSTATE of their own.
 */
public class ConcurrentAppendException extends ConcurrentModificationException {

    public ConcurrentAppendException(String message) {
        super(message);
    }

    public String getSqlState() {
        return "2D521";
    }
}
