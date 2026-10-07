package com.shivamshahi.scheduler.event;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Thread-safe event dispatcher for scheduler lifecycle notifications.
 */
public class EventDispatcher {
    private static final Logger LOGGER = Logger.getLogger(EventDispatcher.class.getName());
    private final List<TaskLifecycleListener> listeners = new CopyOnWriteArrayList<>();

    public void addListener(TaskLifecycleListener listener) {
        listeners.add(Objects.requireNonNull(listener, "Listener cannot be null"));
    }

    public void removeListener(TaskLifecycleListener listener) {
        listeners.remove(listener);
    }

    public void dispatch(TaskSchedulerEvent event) {
        for (TaskLifecycleListener listener : listeners) {
            try {
                listener.onEvent(event);
            } catch (Throwable t) {
                LOGGER.log(Level.WARNING, "Error in TaskLifecycleListener for " + event.getEventType(), t);
            }
        }
    }
}
