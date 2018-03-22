package ru.i_novus.platform.datastorage.temporal.model.criteria;

import ru.i_novus.platform.datastorage.temporal.model.FieldValue;


/**
 * @author lgalimova
 * @since 01.02.2018
 */
public class FieldSearchCriteria {
    private FieldValue value;
    private SearchTypeEnum type = SearchTypeEnum.EXACT;
}
