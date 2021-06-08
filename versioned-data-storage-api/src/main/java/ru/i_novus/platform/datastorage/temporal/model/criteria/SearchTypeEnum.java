package ru.i_novus.platform.datastorage.temporal.model.criteria;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lgalimova
 * @since 21.03.2017
 * MORE - using with Tree field, return all children
 * LESS - using with Tree field, return all parent
 */
public enum SearchTypeEnum {

    EXACT, LIKE,
    MORE, LESS,
    IS_NULL, IS_NOT_NULL
    ;

    private static final Map<String, SearchTypeEnum> TYPE_MAP = new HashMap<>();
    static {
        for (SearchTypeEnum type : SearchTypeEnum.values()) {
           TYPE_MAP.put(type.name(), type);
        }
    }

    public static SearchTypeEnum fromValue(String value) {

        return value != null ? TYPE_MAP.get(value) : SearchTypeEnum.EXACT;
    }
}
