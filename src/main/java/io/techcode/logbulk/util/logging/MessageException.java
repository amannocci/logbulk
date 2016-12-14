package io.techcode.logbulk.util.logging;

/**
 * Simple message exception that contains only a message.
 * Useful to speed up exception handling in some case.
 */
public class MessageException extends Exception {

    // A shared value for an empty stack
    private static final StackTraceElement[] UNASSIGNED_STACK = new StackTraceElement[0];

    // Message
    private final String message;

    /**
     * Create a simple message exception.
     *
     * @param message message exception.
     */
    public MessageException(String message) {
        setStackTrace(UNASSIGNED_STACK);
        this.message = message;
    }

    @Override public String getMessage() {
        return message;
    }

}
