package ru.i_novus.platform.datastorage.temporal.util;

import java.util.Collection;
import java.util.Map;

/**
 * Класс для работы с коллекциями.
 */
public class CollectionUtils {

    private CollectionUtils() {
        throw new UnsupportedOperationException();
    }

    public static boolean isNullOrEmpty(final Collection<?> c) {
        return c == null || c.isEmpty();
    }

    public static boolean isNullOrEmpty(final Map<?, ?> m) {
        return m == null || m.isEmpty();
    }
}
