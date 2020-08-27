package ru.i_novus.platform.versioned_data_storage.pg_impl.util;

public class StringUtils {

    private StringUtils() {
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

}
