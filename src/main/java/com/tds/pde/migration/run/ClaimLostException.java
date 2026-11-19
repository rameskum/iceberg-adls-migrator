package com.tds.pde.migration.run;

/**
 * The run no longer owns its migration's control row, or cannot be sure it still does, so it must publish nothing
 * more and leave the migration's state to whichever run owns it now.
 */
public class ClaimLostException extends RuntimeException {

    public ClaimLostException(String message) {
        super(message);
    }
}
