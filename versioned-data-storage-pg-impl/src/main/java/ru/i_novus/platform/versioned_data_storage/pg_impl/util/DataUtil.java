package ru.i_novus.platform.versioned_data_storage.pg_impl.util;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public class DataUtil {

    private DataUtil() {
        throw new UnsupportedOperationException();
    }

    public static boolean isNullOrEmpty(String s) {
        return s == null || s.isEmpty();
    }

    public static String addDoubleQuotes(String source) {
        return "\"" + source + "\"";
    }

    public static String addSingleQuotes(String source) {
        return "'" + source + "'";
    }

    public static Object truncateDateTo(LocalDateTime date, ChronoUnit unit, Object defaultValue) {
        return date != null ? date.truncatedTo(unit) : defaultValue;
    }
}
