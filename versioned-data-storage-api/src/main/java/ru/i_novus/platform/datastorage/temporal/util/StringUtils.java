package ru.i_novus.platform.datastorage.temporal.util;

import org.apache.commons.text.StringSubstitutor;

import java.util.Map;

public class StringUtils {

    private static final String SUBST_PREFIX = "${";
    private static final String SUBST_SUFFIX = "}";
    private static final String SUBST_DEFAULT = ":";

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

    public static String substitute(String template, Map<String, String> map) {

        StringSubstitutor substitutor = new StringSubstitutor(map, SUBST_PREFIX, SUBST_SUFFIX);
        substitutor.setValueDelimiter(SUBST_DEFAULT);
        return substitutor.replace(template);
    }
}
