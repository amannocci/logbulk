package io.techcode.logbulk.util;

/**
 * A generic event handler.
 */
@FunctionalInterface
public interface HeaderHandler<H, E> {

    /**
     * Something has happened, so handle it.
     *
     * @param headers headers associated to this event.
     * @param event   the event to handle.
     */
    void handle(H headers, E event);

}
