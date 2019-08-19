package ru.i_novus.platform.datastorage.temporal;

import java.util.Collection;
import java.util.Map;

public class CollectionUtils {
    private CollectionUtils() {
        // don't instantiate the object of this class
    }

    public static boolean isNullOrEmpty( final Collection< ? > c ) {
        return c == null || c.isEmpty();
    }

    public static boolean isNullOrEmpty( final Map< ?, ? > m ) {
        return m == null || m.isEmpty();
    }
}
