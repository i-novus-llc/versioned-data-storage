package ru.i_novus.platform.datastorage.temporal.model;

import org.junit.Test;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static org.junit.Assert.*;
import static ru.i_novus.platform.datastorage.temporal.model.DisplayExpression.toPlaceholder;

public class DisplayExpressionTest {

    @Test
    public void testClass() {

        String displayCode = "${code}";
        DisplayExpression codeExpression = new DisplayExpression(displayCode);
        assertEquals(displayCode, codeExpression.getValue());

        String displayName = "${name}";
        DisplayExpression nameExpression = new DisplayExpression("");
        assertEquals("", nameExpression.getValue());
        nameExpression.setValue(displayName);
        assertEquals(displayName, nameExpression.getValue());

        assertNotEquals(codeExpression, nameExpression);
        assertNotEquals(codeExpression.hashCode(), nameExpression.hashCode());
        assertNotEquals(codeExpression.toString(), nameExpression.toString());
    }

    @Test
    public void testGetPlaceholders() {

        String displayValue = "${code:key}=${name:value};${comment}";
        DisplayExpression expression = new DisplayExpression(displayValue);

        Map<String, String> placeholders = expression.getPlaceholders();
        assertNotNull(placeholders);
        assertEquals(3, placeholders.size());

        assertEquals("key", placeholders.get("code"));
        assertEquals("value", placeholders.get("name"));
        assertEquals("", placeholders.get("comment"));
    }

    @Test
    public void testOfField() {

        DisplayExpression expression = DisplayExpression.ofField("code");
        assertEquals("${code}", expression.getValue());
    }

    @Test
    public void testOfFields() {

        assertNull(DisplayExpression.ofFields());

        DisplayExpression expression = DisplayExpression.ofFields("code", "name");
        assertNotNull(expression);
        assertEquals("${code} ${name}", expression.getValue());
    }

    @Test
    public void testOfFieldMap() {

        assertNull(DisplayExpression.ofFields(emptyList()));

        List<Map.Entry<String, String>> fields = new ArrayList<>(3);
        fields.add(new AbstractMap.SimpleEntry<>("code", "key"));
        fields.add(new AbstractMap.SimpleEntry<>("name", "value"));
        fields.add(new AbstractMap.SimpleEntry<>("comment", ""));

        DisplayExpression expression = DisplayExpression.ofFields(fields);
        assertNotNull(expression);
        assertEquals("${code:key} ${name:value} ${comment}", expression.getValue());
    }

    @Test
    public void testToPlaceholder() {

        assertNull(toPlaceholder(null));

        assertEquals("${code}", toPlaceholder("code"));
    }

    @Test
    public void testToPlaceholderWithDefault() {

        assertNull(toPlaceholder(null, ""));

        assertEquals("${code}", toPlaceholder("code", ""));
        assertEquals("${code:default}", toPlaceholder("code", "default"));
    }
}