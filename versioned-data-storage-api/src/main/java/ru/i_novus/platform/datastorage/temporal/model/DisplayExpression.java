package ru.i_novus.platform.datastorage.temporal.model;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DisplayExpression implements Serializable {

    public static final String PLACEHOLDER_START = "${";
    public static final String PLACEHOLDER_END = "}";
    public static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\$\\{(.+?)(:.+)?}");

    private String value;

    public DisplayExpression(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Map<String, String> getPlaceholders() {
        if (value == null) return Collections.emptyMap();
        Map<String, String> placeholders = new HashMap<>();
        Matcher matcher = DisplayExpression.PLACEHOLDER_PATTERN.matcher(value);
        while (matcher.find()) {
            placeholders.put(matcher.group(1), matcher.groupCount() > 1 ? matcher.group(2).substring(1) : "");
        }
        return placeholders;
    }

    public static DisplayExpression ofField(String field) {
        if (field == null) return null;
        return new DisplayExpression(toPlaceholder(field));
    }

    public static DisplayExpression ofFields(String ... field) {
        if (field == null) return null;
        String expression = Stream.of(field).map(DisplayExpression::toPlaceholder).collect(Collectors.joining(" "));
        return new DisplayExpression(expression);
    }

    public static String toPlaceholder(String field) {
        return field == null ? null : PLACEHOLDER_START + field + PLACEHOLDER_END;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DisplayExpression that = (DisplayExpression) o;
        return (value != null ? value.equals(that.value) : that.value == null);
    }

    @Override
    public int hashCode() {
        return value != null ? value.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "DisplayExpression{" +
                "value='" + value + '\'' +
                '}';
    }
}
