package ru.i_novus.platform.versioned_data_storage.pg_impl;

/**
 * @author lgalimova
 * @since 23.03.2018
 */
public class ExceptionCodes {
    public static final String FIELD_NOT_FOUND_EXCEPTION_CODE = "nsi.core.data.loader.fieldNotFound";
    public static final String FIELD_IS_REQUIRED_EXCEPTION_CODE = "nsi.core.data.loader.fieldIsRequired";
    public static final String INCORRECT_FIELD_LENGTH_EXCEPTION_CODE = "nsi.core.data.loader.incorrectFieldLength";
    public static final String BEGIN_END_DATE_EXCEPTION_CODE = "nsi.core.data.loader.beginAndEndDate";
    public static final String EMPTY_KEY_FIELD_EXCEPTION_CODE = "nsi.core.data.loader.emptyKeyField";
    public static final String DUPLICATE_UNIQUE_VALUE_EXCEPTION_CODE = "nsi.core.data.loader.duplicateUniqueValue";
    public static final String DUPLICATE_PRIMARY_VALUE_EXCEPTION_CODE = "nsi.core.data.loader.duplicatePrimaryValue";
    public static final String INCORRECT_DATE_FORMAT_EXCEPTION_CODE = "nsi.core.data.loader.incorrectDateFormat";
    public static final String EMPTY_RECORD_EXCEPTION_CODE = "nsi.core.data.loader.emptyRecord";
    public static final String ACTUAL_VERSION_NOT_FOUND_EXCEPTION_CODE = "nsi.core.dictionary.actual.version.not.found";
    public static final String DATA_INCOMPATIBLE_WITH_TYPE_EXCEPTION_CODE = "nsi.core.data.loader.dataIncompatibleWithType";
    public static final String INVALID_XML_STRUCTURE_EXCEPTION_CODE = "nsi.core.data.xml.structure.error";
    public static final String TABLES_NOT_EQUAL = "nsi.core.data.loader.structure.notEqual";
    public static final String COLUMN_ALREADY_EXISTS = "nsi.core.data.loader.field.duplicate";
    public static final String COLUMN_NOT_EXISTS = "nsi.core.data.field.notExists";
    public static final String SYS_FIELD_CONFLICT = "nsi.core.data.field.sysConflict";
    public static final String INCOMPATIBLE_NEW_DATA_TYPE_EXCEPTION_CODE = "nsi.core.data.loader.incompatibleNewDataType";

}
