package ru.i_novus.platform.datastorage.temporal.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class DisplayExpression implements Serializable {

    public static final Pattern REFERENCE_DISPLAY_PLACEHOLDER_PATTERN = Pattern.compile("\\$\\{(.+?)\\}");

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

    public List<String> getPlaceholders() {
        if (value == null) return Collections.emptyList();
        List<String> placeholders = new ArrayList<>();
        Matcher matcher = DisplayExpression.REFERENCE_DISPLAY_PLACEHOLDER_PATTERN.matcher(value);
        while (matcher.find()) {
            placeholders.add(matcher.group(1));
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
        return field == null ? null : "${" + field + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DisplayExpression that = (DisplayExpression) o;

        return !(value != null ? !value.equals(that.value) : that.value != null);

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
