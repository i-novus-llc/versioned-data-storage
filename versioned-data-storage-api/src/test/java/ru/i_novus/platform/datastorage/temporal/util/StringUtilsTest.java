package ru.i_novus.platform.datastorage.temporal.util;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static ru.i_novus.platform.datastorage.temporal.util.StringUtils.*;

public class StringUtilsTest {

    @Test
    public void testAddDoubleQuotes() {

        assertEquals("\"\"", addDoubleQuotes(""));
        assertEquals("\"abc\"", addDoubleQuotes("abc"));
        assertEquals("\"ab\"cd\"", addDoubleQuotes("ab\"cd"));
    }

    @Test
    public void testAddSingleQuotes() {

        assertEquals("''", addSingleQuotes(""));
        assertEquals("'abc'", addSingleQuotes("abc"));
        assertEquals("'ab'cd'", addSingleQuotes("ab'cd"));
    }

    @Test
    public void testSubstitute() {

        String template = "select ${arg1} from ${arg2}";

        Map<String, String> map = new HashMap<>(2);
        map.put("arg1", "1");
        map.put("arg2", "2");

        String result = substitute(template, map);
        assertEquals("select 1 from 2", result);
    }
}
