package ru.i_novus.platform.datastorage.temporal.model;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Формат отображения.
 * <p>
 * Используется в полях-ссылках.
 * <p>
 * <br>
 * Пример подстановки значений с помощью StringSubstitutor:
 * <pre>
 * {@code
 * StringSubstitutor substitutor = new StringSubstitutor(map, DisplayExpression.PLACEHOLDER_START, DisplayExpression.PLACEHOLDER_END);
 * substitutor.setValueDelimiter(DisplayExpression.PLACEHOLDER_DEFAULT_SEPARATOR);
 * String display = substitutor.replace(displayExpression);
 * }
 * </pre>
 */
public class DisplayExpression implements Serializable {

    private static final String VAR_ESCAPE = "$";
    private static final String VAR_START = "{";
    private static final String VAR_END = "}";
    private static final String VAR_DEFAULT = ":";

    public static final String PLACEHOLDER_START = VAR_ESCAPE + VAR_START;
    public static final String PLACEHOLDER_END = VAR_END;
    public static final String PLACEHOLDER_DEFAULT_DELIMITER = VAR_DEFAULT;

    private static final String PLACEHOLDER_REGEX = "\\" + VAR_ESCAPE + "\\" + VAR_START +
            "(.+?)" + "(" + VAR_DEFAULT + ".*)?" + VAR_END;
    public static final Pattern PLACEHOLDER_PATTERN = Pattern.compile(PLACEHOLDER_REGEX);

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
            placeholders.put(matcher.group(1),
                    matcher.group(2) == null ? "" : matcher.group(2).substring(VAR_DEFAULT.length())
            );
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

    public static String toPlaceholder(String field, String defaultValue) {
        return field == null ? null : PLACEHOLDER_START + field + PLACEHOLDER_DEFAULT_DELIMITER + defaultValue + PLACEHOLDER_END;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DisplayExpression that = (DisplayExpression) o;
        return Objects.equals(value, that.value);
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
