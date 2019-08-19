package ru.i_novus.platform.datastorage.temporal.exception;

import ru.i_novus.components.common.exception.CodifiedException;

public class NotUniqueException extends CodifiedException {
    public NotUniqueException(String code, Object... args) {
        super(code, args);
    }

    public NotUniqueException(String code, Throwable cause, Object... args) {
        super(code, cause, args);
    }
}
