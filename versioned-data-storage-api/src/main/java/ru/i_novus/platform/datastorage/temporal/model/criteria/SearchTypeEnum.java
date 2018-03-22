package ru.i_novus.platform.datastorage.temporal.model.criteria;

/**
 * @author lgalimova
 * @since 21.03.2017
 */
public enum SearchTypeEnum {
    EXACT, LIKE;

    public static SearchTypeEnum getByText(String value) {
        for (SearchTypeEnum searchTypeEnum : values()) {
            if (searchTypeEnum.name().equals(value))
                return searchTypeEnum;
        }
        return null;
    }
}
