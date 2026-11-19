package com.tds.pde.migration.testing;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.util.List;
import org.slf4j.LoggerFactory;

/** Collects the formatted messages logged under a logger name while open. */
public final class LogCapture implements AutoCloseable {

    private final Logger logger;
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

    public LogCapture(String loggerName) {
        this.logger = (Logger) LoggerFactory.getLogger(loggerName);
        appender.start();
        logger.addAppender(appender);
    }

    public List<String> messages() {
        return appender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
    }

    public List<String> messagesAt(Level level) {
        return appender.list.stream().filter(event -> event.getLevel() == level)
                .map(ILoggingEvent::getFormattedMessage).toList();
    }

    public List<String> messagesStartingWith(String prefix) {
        return messages().stream().filter(message -> message.startsWith(prefix)).toList();
    }

    @Override
    public void close() {
        logger.detachAppender(appender);
    }
}
