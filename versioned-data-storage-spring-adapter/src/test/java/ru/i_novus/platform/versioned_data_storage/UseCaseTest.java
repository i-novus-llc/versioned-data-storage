package ru.i_novus.platform.versioned_data_storage;

import net.n2oapp.criteria.api.CollectionPage;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import ru.i_novus.platform.datastorage.temporal.enums.DiffStatusEnum;
import ru.i_novus.platform.datastorage.temporal.enums.FieldType;
import ru.i_novus.platform.datastorage.temporal.exception.NotUniqueException;
import ru.i_novus.platform.datastorage.temporal.model.*;
import ru.i_novus.platform.datastorage.temporal.model.criteria.*;
import ru.i_novus.platform.datastorage.temporal.model.value.*;
import ru.i_novus.platform.datastorage.temporal.service.*;
import ru.i_novus.platform.versioned_data_storage.config.VersionedDataStorageConfig;
import ru.i_novus.platform.versioned_data_storage.pg_impl.model.IntegerField;
import ru.i_novus.platform.versioned_data_storage.pg_impl.model.StringField;

import java.math.BigInteger;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.*;
import static ru.i_novus.platform.datastorage.temporal.model.StorageConstants.*;
import static ru.i_novus.platform.datastorage.temporal.util.StorageUtils.toStorageCode;
import static ru.i_novus.platform.versioned_data_storage.DataTestUtils.*;
import static ru.i_novus.platform.versioned_data_storage.pg_impl.dao.QueryConstants.TRANSACTION_ROW_LIMIT;

/**
 * Created by tnurdinov on 08.06.2018.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {JpaTestConfig.class, VersionedDataStorageConfig.class})
public class UseCaseTest {

    private static final Logger logger = LoggerFactory.getLogger(UseCaseTest.class);

    private static final ZoneId UNIVERSAL_TIMEZONE = ZoneId.of("UTC");

    @Autowired
    private FieldFactory fieldFactory;

    @Autowired
    private StorageCodeService storageCodeService;

    @Autowired
    private DraftDataService draftDataService;

    @Autowired
    private SearchDataService searchDataService;

    @Autowired
    private CompareDataService compareDataService;

    private LocalDateTime now() {
        return LocalDateTime.now(UNIVERSAL_TIMEZONE);
    }

    private LocalDateTime multiply(LocalDateTime localDateTime, long factor) {
        LocalDateTime epochDateTime = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC);
        long seconds = ChronoUnit.SECONDS.between(epochDateTime, localDateTime);
        return localDateTime.plusSeconds(seconds * factor);
    }

    /**
     * 1 этап. Создание черновика D_A с данными
     * 2 этап. Создание хранилища S_A из черновика D_A
     * 3 этап. Создание черновика D_B с данными и с ссылкой на S_A
     * 4 этап. Создание хранилища S_B из черновика D_B
     *
     * @throws Exception
     */
    @Test
    public void testCreateReferenceStorage() throws Exception {
        logger.info("<<<<<<<<<<<<<<< 1 этап >>>>>>>>>>>>>>>>>>>>>");
        List<Field> d_a_fields = new ArrayList<>();
        Field d_a_id = fieldFactory.createField(FIELD_ID_CODE, FieldType.INTEGER);
        Field d_a_name = fieldFactory.createField(FIELD_NAME_CODE, FieldType.STRING);
        Field d_a_dateCol = fieldFactory.createField("DATE_COL", FieldType.DATE);
        Field d_a_boolCol = fieldFactory.createField("BOOL_COL", FieldType.BOOLEAN);
        Field d_a_floatCol = fieldFactory.createField("FLOAT_COL", FieldType.FLOAT);
        d_a_fields.add(d_a_id);
        d_a_fields.add(d_a_name);
        d_a_fields.add(d_a_dateCol);
        d_a_fields.add(d_a_boolCol);
        d_a_fields.add(d_a_floatCol);

        String d_a_draftCode = draftDataService.createDraft(d_a_fields);
        List<RowValue> d_a_rows = new ArrayList<>();
        d_a_rows.add(new LongRowValue(
                d_a_id.valueOf(BigInteger.valueOf(1)),
                d_a_name.valueOf("test"),
                d_a_dateCol.valueOf(LocalDate.now()),
                d_a_boolCol.valueOf(true),
                d_a_floatCol.valueOf(5f)));
        d_a_rows.add(new LongRowValue(
                d_a_id.valueOf(BigInteger.valueOf(2)),
                d_a_name.valueOf("test2"),
                d_a_dateCol.valueOf(LocalDate.now()),
                d_a_boolCol.valueOf(true),
                d_a_floatCol.valueOf(4f)));
        draftDataService.addRows(d_a_draftCode, d_a_rows);

        StorageDataCriteria criteria = new StorageDataCriteria(d_a_draftCode, null, null, d_a_fields, emptySet(), null);
        Collection<RowValue> d_a_actualRows = searchDataService.getPagedData(criteria).getCollection();
        assertRows(d_a_rows, d_a_actualRows);
        logger.info("<<<<<<<<<<<<<<< 1 этап завершен >>>>>>>>>>>>>>>>>>>>>");

        logger.info("<<<<<<<<<<<<<<< 2 этап >>>>>>>>>>>>>>>>>>>>>");
        LocalDateTime s_a_publishTime = now();
        LocalDateTime beforeSAPublishDate = s_a_publishTime.minus(1, ChronoUnit.DAYS);
        String s_a_storageCode = draftDataService.applyDraft(null, d_a_draftCode, s_a_publishTime);

        criteria = new StorageDataCriteria(s_a_storageCode, beforeSAPublishDate, null, d_a_fields, emptySet(), null);
        Collection<RowValue> s_a_actualRows = searchDataService.getData(criteria);
        assertEquals(0, s_a_actualRows.size());

        criteria = new StorageDataCriteria(s_a_storageCode, s_a_publishTime, null, d_a_fields, emptySet(), null);
        s_a_actualRows = searchDataService.getData(criteria);
        assertRows(d_a_rows, s_a_actualRows);
        logger.info("<<<<<<<<<<<<<<< 2 этап завершен >>>>>>>>>>>>>>>>>>>>>");

        logger.info("<<<<<<<<<<<<<<< 3 этап >>>>>>>>>>>>>>>>>>>>>");
        Field d_b_id = fieldFactory.createField(FIELD_ID_CODE, FieldType.INTEGER);
        Field d_b_name = fieldFactory.createField(FIELD_NAME_CODE, FieldType.STRING);
        Field d_b_ref = fieldFactory.createField("REF", FieldType.REFERENCE);
        List<Field> d_b_fields = asList(d_b_id, d_b_name, d_b_ref);
        String d_b_draftCode = draftDataService.createDraft(d_b_fields);
        RowValue d_b_rowValue = new LongRowValue(
                d_b_id.valueOf(BigInteger.valueOf(1)),
                d_b_name.valueOf("name"),
                d_b_ref.valueOf(new Reference(s_a_storageCode, s_a_publishTime, d_a_id.getName(), d_a_name.getName(), "1", "test"))
        );
        draftDataService.addRows(d_b_draftCode, singletonList(d_b_rowValue));

        criteria = new StorageDataCriteria(d_b_draftCode, null, null, d_b_fields, emptySet(), "name");
        List<RowValue> d_b_actualRows = searchDataService.getData(criteria);
        Object systemId = d_b_actualRows.get(0).getSystemId();

        d_b_rowValue = new LongRowValue(
                d_b_id.valueOf(BigInteger.valueOf(1)),
                d_b_name.valueOf("name"),
                d_b_ref.valueOf(new Reference(s_a_storageCode, s_a_publishTime, d_a_id.getName(), d_a_name.getName(), "2"))
        );
        d_b_rowValue.setSystemId(systemId);
        draftDataService.updateRows(d_b_draftCode, singletonList(d_b_rowValue));

        criteria = new StorageDataCriteria(d_b_draftCode, null, null, d_b_fields, emptySet(), "name");
        d_b_actualRows = searchDataService.getData(criteria);
        d_b_rowValue.getFieldValues().forEach(value -> {
            if (value instanceof ReferenceFieldValue) {
                ((ReferenceFieldValue) value).getValue().setDisplayValue("test2");
            }
        });
        assertRows(singletonList(d_b_rowValue), d_b_actualRows);
        logger.info("<<<<<<<<<<<<<<< 3 этап завершен >>>>>>>>>>>>>>>>>>>>>");

        logger.info("<<<<<<<<<<<<<<< 4 этап >>>>>>>>>>>>>>>>>>>>>");
        LocalDateTime s_b_publishTime = now();
        String s_b_storageCode = draftDataService.applyDraft(null, d_b_draftCode, s_b_publishTime);

        criteria = new StorageDataCriteria(s_b_storageCode, null, null, d_b_fields, emptySet(), null);
        Collection<RowValue> s_b_actualRows = searchDataService.getData(criteria);
        assertRows(singletonList(d_b_rowValue), s_b_actualRows);
        logger.info("<<<<<<<<<<<<<<< 4 этап завершен >>>>>>>>>>>>>>>>>>>>>");

        ReferenceFieldValue referenceFieldValue = (ReferenceFieldValue)d_b_ref.valueOf(
                new Reference(s_a_storageCode, s_a_publishTime, d_a_id.getName(),
                        new DisplayExpression("${" + d_a_name.getName() + "}"), null, null)
        );
    }

    @Test
    public void testUpdateDraft() {
        String existingStorageCode = createStorage();
        List<Field> fields = new ArrayList<>();
        Field code = fieldFactory.createField(FIELD_CODE_CODE, FieldType.STRING);
        Field name = fieldFactory.createField(FIELD_NAME_CODE, FieldType.STRING);
        Field ref = fieldFactory.createField("REF", FieldType.STRING);
        fields.add(code);
        fields.add(name);
        fields.add(ref);

        String draftCode = draftDataService.createDraft(fields);
        Field ref_new = fieldFactory.createField("REF", FieldType.REFERENCE);
        draftDataService.updateField(draftCode, ref_new);
        fields.remove(ref);
        fields.add(ref_new);
        Field date_col = fieldFactory.createField("DATE_COL", FieldType.DATE);
        draftDataService.addField(draftCode, date_col);
        fields.add(date_col);

        // Добавление строк.
        List<RowValue> rows = new ArrayList<>();
        ReferenceFieldValue oldRefValue = new ReferenceFieldValue(ref.getName(), new Reference(existingStorageCode, now(), FIELD_ID_CODE, FIELD_NAME_CODE, "1", "test"));
        RowValue rowValue = new LongRowValue(
                code.valueOf("001"),
                name.valueOf("name"),
                oldRefValue,
                date_col.valueOf(LocalDate.now()));
        rows.add(rowValue);
        draftDataService.addRows(draftCode, rows);

        StorageDataCriteria criteria = new StorageDataCriteria(draftCode, null, null, fields, emptySet(), null);
        List<RowValue> actualRows = searchDataService.getData(criteria);
        assertRows(rows, actualRows);

        // Обновление значения ссылки.
        List<Object> updatedIds = asList(actualRows.get(0).getSystemId(), -1);
        String newDisplayValue = "test3";
        // - отображаемое значение ссылки не должно меняться, т.к. данные не менялись в existingStorageCode.
        ReferenceFieldValue updatedRefValue = new ReferenceFieldValue(ref.getName(), new Reference(existingStorageCode, now(), FIELD_ID_CODE, FIELD_NAME_CODE, null, null));
        draftDataService.updateReferenceInRows(draftCode, updatedRefValue, updatedIds);

        criteria = new StorageDataCriteria(draftCode, null, null, fields, emptySet(), null);
        actualRows = searchDataService.getData(criteria);
        assertRows(rows, actualRows);

        // - меняются данные в existingStorageCode.
        Field existingFieldId = fieldFactory.createField(FIELD_ID_CODE, FieldType.INTEGER);
        Field existingFieldName = fieldFactory.createField(FIELD_NAME_CODE, FieldType.STRING);

        criteria = new StorageDataCriteria(existingStorageCode, null, null, singletonList(existingFieldId), emptySet(), null);
        List<RowValue> existingRows = searchDataService.getData(criteria);
        assertTrue(existingRows.size() > 0);

        Object existingSystemId = existingRows.get(0).getSystemId();
        FieldValue existingFieldValue = new StringFieldValue(existingFieldName.getName(), newDisplayValue);
        RowValue updateRowValue = new LongRowValue((Long)existingSystemId, singletonList(existingFieldValue));
        draftDataService.updateRows(existingStorageCode, singletonList(updateRowValue));
        // - отображаемое значение ссылки должно измениться, т.к. изменились данные в existingStorageCode.
        draftDataService.updateReferenceInRows(draftCode, updatedRefValue, updatedIds);

        criteria = new StorageDataCriteria(draftCode, null, null, fields, emptySet(), null);
        actualRows = searchDataService.getData(criteria);
        ReferenceFieldValue fieldValue = (ReferenceFieldValue)rows.get(0).getFieldValue(ref.getName());
        fieldValue.getValue().setDisplayValue(newDisplayValue);
        assertRows(rows, actualRows);

        String storageCode = draftDataService.applyDraft(null, draftCode, now());

        criteria = new StorageDataCriteria(storageCode, null, null, fields, emptySet(), null);
        actualRows = searchDataService.getData(criteria);
        assertRows(rows, actualRows);
    }

    private void assertRows(List<RowValue> expectedRows, Collection<RowValue> actualRows) {

        assertEquals("result size not equals", expectedRows.size(), actualRows.size());

        assertTrue(
                "not equals actualRows: \n"
                        + actualRows.stream().map(RowValue::toString).collect(joining(", "))
                        + " \n and expected rows: \n"
                        + expectedRows.stream().map(RowValue::toString).collect(joining(", "))
                , actualRows.stream().anyMatch(actualRow ->
                        expectedRows.stream().anyMatch(expectedRow ->
                                equalsFieldValues(expectedRow.getFieldValues(), actualRow.getFieldValues())
                        ))
        );
    }

    private boolean equalsFieldValues(List<FieldValue> values1, List<FieldValue> values2) {
        if (values1 == values2)
            return true;
        if (values1 == null || values2 == null || values1.size() != values2.size())
            return false;

        for (FieldValue val1 : values1) {
            boolean isPresent = values2.stream().anyMatch(val2 -> {
                if (val2 == val1)
                    return true;
                if (val2.getField().equals(val1.getField()))
                    if (val1 instanceof ReferenceFieldValue) {
                        return ((ReferenceFieldValue) val2).getValue().getValue().equals(((ReferenceFieldValue) val1).getValue().getValue())
                                && ((ReferenceFieldValue) val2).getValue().getDisplayValue().equals(((ReferenceFieldValue) val1).getValue().getDisplayValue());
                    } else {
                        return val2.equals(val1);
                    }
                return false;

            });
            if (!isPresent) {
                return false;
            }

        }

        return true;

    }

    private String createStorage() {
        List<Field> d_a_fields = new ArrayList<>();
        Field d_a_id = fieldFactory.createField(FIELD_ID_CODE, FieldType.INTEGER);
        Field d_a_name = fieldFactory.createField(FIELD_NAME_CODE, FieldType.STRING);
        Field d_a_dateCol = fieldFactory.createField("DATE_COL", FieldType.DATE);
        Field d_a_boolCol = fieldFactory.createField("BOOL_COL", FieldType.BOOLEAN);
        Field d_a_floatCol = fieldFactory.createField("FLOAT_COL", FieldType.FLOAT);
        d_a_fields.add(d_a_id);
        d_a_fields.add(d_a_name);
        d_a_fields.add(d_a_dateCol);
        d_a_fields.add(d_a_boolCol);
        d_a_fields.add(d_a_floatCol);

        String d_a_draftCode = draftDataService.createDraft(d_a_fields);
        List<RowValue> d_a_rows = new ArrayList<>();
        d_a_rows.add(new LongRowValue(
                d_a_id.valueOf(BigInteger.valueOf(1)),
                d_a_name.valueOf("test"),
                d_a_dateCol.valueOf(LocalDate.now()),
                d_a_boolCol.valueOf(true),
                d_a_floatCol.valueOf(5f)));
        d_a_rows.add(new LongRowValue(
                d_a_id.valueOf(BigInteger.valueOf(2)),
                d_a_name.valueOf("test2"),
                d_a_dateCol.valueOf(LocalDate.now()),
                d_a_boolCol.valueOf(true),
                d_a_floatCol.valueOf(4f)));
        draftDataService.addRows(d_a_draftCode, d_a_rows);
        return draftDataService.applyDraft(null, d_a_draftCode, now());
    }

    @Test
    public void testCopyAllData() {

        List<Field> fields = new ArrayList<>();
        Field idField = fieldFactory.createField(FIELD_ID_CODE, FieldType.INTEGER);
        Field nameField = fieldFactory.createField(FIELD_NAME_CODE, FieldType.STRING);
        fields.add(idField);
        fields.add(nameField);

        String sourceName = draftDataService.createDraft(fields);
        String sourceCode = toStorageCode(null, sourceName);

        List<RowValue> sourceRows = new ArrayList<>();
        IntStream.range(0, TRANSACTION_ROW_LIMIT * 2).forEach(i -> {
            sourceRows.add(new LongRowValue(
                    idField.valueOf(BigInteger.valueOf(i)),
                    nameField.valueOf("test_" + i)));
        });
        draftDataService.addRows(sourceCode, sourceRows);

        String targetName = draftDataService.createDraft(TEST_SCHEMA_NAME, fields);
        String targetCode = toStorageCode(TEST_SCHEMA_NAME, targetName);
        draftDataService.copyAllData(sourceCode, targetCode);

        StorageDataCriteria criteria = new StorageDataCriteria(targetCode, null, null,
                singletonList(idField), emptySet(), null);
        criteria.setPage(DataCriteria.MIN_PAGE);
        criteria.setSize(DataCriteria.MIN_SIZE);

        Collection<RowValue> targetRows = searchDataService.getPagedData(criteria).getCollection();
        assertEquals(sourceRows.size(), criteria.getCount().intValue());
        assertEquals(1, targetRows.size());

        RowValue targetRow = targetRows.iterator().next();
        assertTrue(sourceRows.stream()
                .anyMatch(sourceRow ->
                        sourceRow.getFieldValue(FIELD_ID_CODE).getValue()
                                .equals(targetRow.getFieldValue(FIELD_ID_CODE).getValue()))
        );
    }

    /**
     * Запись двух одинаковых строк в один черновик
     * Ожидается ошибка NotUniqueException - нарушение уникальности строк в БД ("SYS_HASH")
     */
    @Test
    public void testCreateUniqueHash() {
        List<Field> d_a_fields = new ArrayList<>();
        Field d_a_id = fieldFactory.createField(FIELD_ID_CODE, FieldType.INTEGER);
        Field d_a_name = fieldFactory.createField(FIELD_NAME_CODE, FieldType.STRING);
        d_a_fields.add(d_a_id);
        d_a_fields.add(d_a_name);

        String d_a_draftCode = draftDataService.createDraft(d_a_fields);
        List<RowValue> d_a_rows = new ArrayList<>();
        d_a_rows.add(new LongRowValue(
                d_a_id.valueOf(BigInteger.valueOf(1)),
                d_a_name.valueOf("test")));
        draftDataService.addRows(d_a_draftCode, d_a_rows);
        try {
            draftDataService.addRows(d_a_draftCode, d_a_rows);
            Assert.fail("Two equals row error");
        } catch (NotUniqueException ignored) {
        }
    }

    /**
     * Удаление поля из черновика
     * Ожидается: обновление SYS_HASH и FTS у существующих записей
     */
    @Test
    public void testDeleteField() {
        List<Field> fields = new ArrayList<>();
        Field id = fieldFactory.createField(FIELD_ID_CODE, FieldType.INTEGER);
        Field name = fieldFactory.createField(FIELD_NAME_CODE, FieldType.STRING);
        Field date = fieldFactory.createField("DATE", FieldType.DATE);
        fields.add(id);
        fields.add(name);
        fields.add(date);

        String storageCode = draftDataService.createDraft(fields);
        List<RowValue> rows = singletonList(new LongRowValue(
                id.valueOf(BigInteger.valueOf(1)),
                name.valueOf("test"),
                date.valueOf(LocalDate.now())));
        draftDataService.addRows(storageCode, rows);

        List<Field> hashField = singletonList(fieldFactory.createField(SYS_HASH, FieldType.STRING));
        StorageDataCriteria criteria = new StorageDataCriteria(storageCode, null, null, hashField, emptySet(), null);

        RowValue hashBeforeDelete = searchDataService.getData(criteria).get(0);
        draftDataService.deleteField(storageCode, FIELD_NAME_CODE);
        RowValue dataAfterDelete = searchDataService.getData(criteria).get(0);
        assertNotEquals(hashBeforeDelete, dataAfterDelete);

        fields.remove(name);

        criteria = new StorageDataCriteria(storageCode, null, null, fields, emptySet(), "test");
        List<RowValue> searchDeletedField = searchDataService.getData(criteria);
        assertTrue(searchDeletedField.isEmpty());

        criteria = new StorageDataCriteria(storageCode, null, null, fields, emptySet(), "1");
        List<RowValue> searchExistingField = searchDataService.getData(criteria);
        assertTrue(!searchExistingField.isEmpty());

    }

    /**
     * Добавление поля из черновика
     * Ожидается: обновление SYS_HASH у существующих записей
     */
    @Test
    public void testAddField() {
        List<Field> fields = new ArrayList<>();
        Field id = fieldFactory.createField(FIELD_ID_CODE, FieldType.INTEGER);
        Field name = fieldFactory.createField(FIELD_NAME_CODE, FieldType.STRING);
        fields.add(id);
        fields.add(name);

        String storageCode = draftDataService.createDraft(fields);
        List<RowValue> rows = singletonList(new LongRowValue(
                id.valueOf(BigInteger.valueOf(1)),
                name.valueOf("test")));
        draftDataService.addRows(storageCode, rows);

        List<Field> hashField = singletonList(fieldFactory.createField(SYS_HASH, FieldType.STRING));
        StorageDataCriteria criteria = new StorageDataCriteria(storageCode, null, null, hashField, emptySet(), null);

        RowValue hashBeforeAdd = searchDataService.getData(criteria).get(0);
        Field date = fieldFactory.createField("DATE", FieldType.DATE);
        draftDataService.addField(storageCode, date);
        RowValue dataAfterAdd = searchDataService.getData(criteria).get(0);

        assertNotEquals(hashBeforeAdd, dataAfterAdd);
    }

    /*
     * testing manipulations while publishing a draft with closeDate
     */
    @Test
    public void testApplyDraftWithCloseDate() {
        Long time_sec_diff = 150000L;

        List<Field> fields = new ArrayList<>();
        Field id = fieldFactory.createField(FIELD_ID_CODE, FieldType.INTEGER);
        Field code = fieldFactory.createField(FIELD_CODE_CODE, FieldType.STRING);
        Field name = fieldFactory.createField(FIELD_NAME_CODE, FieldType.STRING);
        fields.add(id);
        fields.add(code);
        fields.add(name);
        final List<Field> withSysDateFields = asList(id, code, name, fieldFactory.createField(SYS_PUBLISHTIME, FieldType.STRING), fieldFactory.createField(SYS_CLOSETIME, FieldType.STRING));
        List<RowValue> rows = new ArrayList<>();
        rows.add(new LongRowValue(
                id.valueOf(BigInteger.valueOf(1)),
                code.valueOf("001"),
                name.valueOf("name1")));
        rows.add(new LongRowValue(
                id.valueOf(BigInteger.valueOf(2)),
                code.valueOf("002"),
                name.valueOf("name2")));
        rows.add(new LongRowValue(
                id.valueOf(BigInteger.valueOf(3)),
                code.valueOf("003"),
                name.valueOf("name3")));
        String draftCode = draftDataService.createDraft(fields);
        List<RowValue> rows1 = new ArrayList<>();
        rows1.add(rows.get(0));
        rows1.add(rows.get(1));
        draftDataService.addRows(draftCode, rows1);
        LocalDateTime publishDate1 = now();
        LocalDateTime closeDate1 = publishDate1.plusSeconds(time_sec_diff);
        String storageCode = draftDataService.applyDraft(null, draftCode, publishDate1, closeDate1);

        StorageDataCriteria criteria = new StorageDataCriteria(storageCode, publishDate1, closeDate1, fields, emptySet(), null);
        List<RowValue> actualRows = searchDataService.getData(criteria);
//        проверка в опубликованной версии
        assertRows(rows1, actualRows);

        // "1" (pT1, cT1), "2" (pT1, cT1) общее кол-во записей в таблице версий
        criteria = new StorageDataCriteria(storageCode, null, null, fields, emptySet(), null);
        assertEquals(2, searchDataService.getData(criteria).size());

        // pT1 < pT2 < cT2 < cT1
        draftCode = draftDataService.createDraft(fields);
        List<RowValue> rows2 = new ArrayList<>();
        rows2.add(rows.get(1));
        rows2.add(rows.get(2));
        //удаляем rows.get(0)
        draftDataService.addRows(draftCode, rows2);
        LocalDateTime publishDate2 = publishDate1.plusSeconds(time_sec_diff / 100L);
        LocalDateTime closeDate2 = publishDate1.plusSeconds(time_sec_diff / 100L * 50);
        String storageCode2 = draftDataService.applyDraft(storageCode, draftCode, publishDate2, closeDate2);

        criteria = new StorageDataCriteria(storageCode2, publishDate2, closeDate2, fields, emptySet(), null);
        actualRows = searchDataService.getData(criteria);
        assertRows(rows2, actualRows);

        criteria = new StorageDataCriteria(storageCode2, null, null, withSysDateFields, emptySet(), null);
        System.out.println("publishDate2 = " + publishDate2 + "\n closeDate2 = " + closeDate2 + "" +
                "\npublishDate1 = " + publishDate1 + "\n closeDate1 = " + closeDate1 + "" +
                " \n rows = " + searchDataService.getData(criteria).toString());

        // "1" (pT1, pT2), "1" (cT2, cT1), "2" (pT1, cT1), "3" (pT2, cT2)
        criteria = new StorageDataCriteria(storageCode2, null, null, fields, emptySet(), null);
        assertEquals("incorrect total rows count in version table", 4, searchDataService.getData(criteria).size());

        // pT1 < pT2 < pT3 < cT3 < cT2 < cT1
        draftCode = draftDataService.createDraft(fields);
        List<RowValue> rows3 = new ArrayList<>();
        rows3.add(rows.get(0));
        draftDataService.addRows(draftCode, rows3);
        LocalDateTime publishDate3 = publishDate1.plusSeconds(time_sec_diff / 10L);
        LocalDateTime closeDate3 = publishDate1.plusSeconds(time_sec_diff / 10L * 4);
        String storageCode3 = draftDataService.applyDraft(storageCode2, draftCode, publishDate3, closeDate3);

        criteria = new StorageDataCriteria(storageCode3, publishDate3, closeDate3, fields, emptySet(), null);
        actualRows = searchDataService.getData(criteria);

        criteria = new StorageDataCriteria(storageCode3, publishDate3, closeDate3, withSysDateFields, emptySet(), null);
        System.out.println("publishDate3 = " + publishDate3 +
                "\ncloseDate3 = " + closeDate3 + " \n" +
                searchDataService.getData(criteria));
        assertRows(rows3, actualRows);

        // "1" (pT1, pT2), "1" (pT3, cT3), "1" (cT2, cT1), "2" (pT1, pT3), "2" (cT3, cT1), "3" (pT2, pT3), "3" (cT3, cT2)
        criteria = new StorageDataCriteria(storageCode3, null, null, fields, emptySet(), null);
        assertEquals(7, searchDataService.getData(criteria).size());

        // pT1 < pT2 <= pT4 < cT4 <= pT3 < cT3 < cT2 < cT1
        draftCode = draftDataService.createDraft(fields);
        List<RowValue> rows4 = new ArrayList<>(rows);
        draftDataService.addRows(draftCode, rows4);
        LocalDateTime publishDate4 = publishDate1.plusSeconds(time_sec_diff / 100L);
        LocalDateTime closeDate4 = publishDate1.plusSeconds(time_sec_diff / 10L);
        String storageCode4 = draftDataService.applyDraft(storageCode3, draftCode, publishDate4, closeDate4);

        criteria = new StorageDataCriteria(storageCode4, publishDate4, closeDate4, fields, emptySet(), null);
        actualRows = searchDataService.getData(criteria);
        assertRows(rows4, actualRows);

        // "1" (pT1, cT3), "1" (cT2, cT1), "2" (pT1, pT3), "2" (cT3, cT1), "3" (pT2, pT3) = (pT4, cT4), "3" (cT3, cT2)
        criteria = new StorageDataCriteria(storageCode4, null, null, fields, emptySet(), null);
        assertEquals(6, searchDataService.getData(criteria).size());
        FieldSearchCriteria fieldSearchCriteria = new FieldSearchCriteria(id, SearchTypeEnum.EXACT, singletonList(BigInteger.valueOf(1)));

        //test that there are exactly two rows with id=1 (point row was deleted, one was remained unmodified, one was added
        criteria = new StorageDataCriteria(storageCode4, null, null, fields, singletonList(fieldSearchCriteria), null);
        assertEquals(2, searchDataService.getData(criteria).size());

        draftCode = draftDataService.createDraft(fields);
        draftDataService.addRows(draftCode, rows);
        storageCode = draftDataService.applyDraft(null, draftCode, publishDate1, null);

        criteria = new StorageDataCriteria(storageCode, publishDate1, null, fields, emptySet(), null);
        actualRows = searchDataService.getData(criteria);
        assertRows(rows, actualRows);

        draftCode = draftDataService.createDraft(fields);
        draftDataService.addRows(draftCode, rows);
        String storageCode1 = draftDataService.applyDraft(storageCode, draftCode, publishDate2, null);

        criteria = new StorageDataCriteria(storageCode1, publishDate1, null, fields, emptySet(), null);
        actualRows = searchDataService.getData(criteria);
        assertRows(rows, actualRows);

        criteria = new StorageDataCriteria(storageCode1, publishDate2, null, fields, emptySet(), null);
        actualRows = searchDataService.getData(criteria);
        assertRows(rows, actualRows);

    }

    /*
     * testing get data difference between two published versions in one storage
     */
    @Test
    public void testGetDataDifferenceForTwoPublishedVersionsWithSameStorage() {
        List<Field> fields = new ArrayList<>();
        Field id = fieldFactory.createField(FIELD_ID_CODE, FieldType.INTEGER);
        Field code = fieldFactory.createField(FIELD_CODE_CODE, FieldType.STRING);
        fields.add(id);
        fields.add(code);
        List<RowValue> rows = new ArrayList<>();
        rows.add(new LongRowValue(
                id.valueOf(BigInteger.valueOf(1)),
                code.valueOf("001")));
        rows.add(new LongRowValue(
                id.valueOf(BigInteger.valueOf(2)),
                code.valueOf("002")));
        rows.add(new LongRowValue(
                id.valueOf(BigInteger.valueOf(3)),
                code.valueOf("003")));
        rows.add(new LongRowValue(
                id.valueOf(BigInteger.valueOf(4)),
                code.valueOf("004")));

        String draftCode = draftDataService.createDraft(fields);
        draftDataService.addRows(draftCode, asList(rows.get(0), rows.get(1), rows.get(2)));
        LocalDateTime publishDate1 = now();
        LocalDateTime closeDate1 = multiply(publishDate1, 3);
        String storageCode = draftDataService.applyDraft(null, draftCode, publishDate1, closeDate1);

        draftCode = draftDataService.createDraft(fields);

        rows.get(2).getFieldValue(code.getName()).setValue("003_1");
        draftDataService.addRows(draftCode, asList(rows.get(1), rows.get(2), rows.get(3)));
        LocalDateTime publishDate2 = multiply(publishDate1, 2);
        LocalDateTime closeDate2 = closeDate1;
        String storageCode2 = draftDataService.applyDraft(storageCode, draftCode, publishDate2, closeDate2);

        Set<List<FieldSearchCriteria>> fieldValues = rows
                .stream()
                .map(row -> singletonList(
                        new FieldSearchCriteria(id, SearchTypeEnum.EXACT, singletonList(row.getFieldValue(id.getName()).getValue()))
                        )
                )
                .collect(Collectors.toSet());

        // WARN: bug? : replace first publishDate2 with closeDate1
        CompareDataCriteria compareDataCriteria = new CompareDataCriteria(storageCode2, null, publishDate1, publishDate2, publishDate2, closeDate2, fields, singletonList(FIELD_ID_CODE), fieldValues);
        DataDifference actualDataDifference = compareDataService.getDataDifference(compareDataCriteria);
        List<DiffRowValue> expectedDiffRowValues = new ArrayList<>();
        expectedDiffRowValues.add(new DiffRowValue(
                fields.stream()
                        .map(field -> new DiffFieldValue<>(field, rows.get(0).getFieldValue(field.getName()).getValue(), null, DiffStatusEnum.DELETED))
                        .collect(toList()),
                DiffStatusEnum.DELETED));
        expectedDiffRowValues.add(new DiffRowValue(
                fields.stream()
                        .map(field -> new DiffFieldValue<>(field,
                                field.equals(code) ? "003" : null,
                                rows.get(2).getFieldValue(field.getName()).getValue(),
                                field.equals(code) ? DiffStatusEnum.UPDATED : null))
                        .collect(toList()),
                DiffStatusEnum.UPDATED));
        expectedDiffRowValues.add(new DiffRowValue(
                fields.stream()
                        .map(field -> new DiffFieldValue<>(field,
                                null,
                                rows.get(3).getFieldValue(field.getName()).getValue(),
                                DiffStatusEnum.INSERTED))
                        .collect(toList()),
                DiffStatusEnum.INSERTED));
        assertDiffRowValues(expectedDiffRowValues, (List<DiffRowValue>) actualDataDifference.getRows().getCollection());
    }

    /*
     * testing get data difference between two published versions when they contain null-values
     * https://jira.i-novus.ru/browse/RDM-406
     * rows:
     * 1 - row deleted
     * 2 - value deleted (newValue = null)
     * 3 - updated (oldValue != null, newValue != null, oldValue != newValue)
     * 4 - unchanged (oldValue = newValue != null)
     * 5 - unchanged (oldValue = newValue = null)
     * 6 - value inserted (oldValue = null)
     * 7 - row inserted
     */
    @Test
    public void testGetDataDifferenceForVersionsWithNullValues() {
        List<Field> fields = new ArrayList<>();
        Field id = fieldFactory.createField(FIELD_ID_CODE, FieldType.INTEGER);
        Field code = fieldFactory.createField(FIELD_CODE_CODE, FieldType.STRING);
        fields.add(id);
        fields.add(code);
        List<RowValue> rows = new ArrayList<>();
        rows.add(new LongRowValue(
                id.valueOf(BigInteger.valueOf(1)),
                code.valueOf("001")));
        rows.add(new LongRowValue(
                id.valueOf(BigInteger.valueOf(2)),
                code.valueOf("002")));
        rows.add(new LongRowValue(
                id.valueOf(BigInteger.valueOf(3)),
                code.valueOf("003")));
        rows.add(new LongRowValue(
                id.valueOf(BigInteger.valueOf(4)),
                code.valueOf("004")));
        rows.add(new LongRowValue(
                id.valueOf(BigInteger.valueOf(5)),
                new StringFieldValue(code.getName(), null)));
        rows.add(new LongRowValue(
                id.valueOf(BigInteger.valueOf(6)),
                new StringFieldValue(code.getName(), null)));
        rows.add(new LongRowValue(
                id.valueOf(BigInteger.valueOf(7)),
                code.valueOf("007_1")));

        String draftCode = draftDataService.createDraft(fields);
        draftDataService.addRows(draftCode, rows.subList(0, 6));
        LocalDateTime publishDate1 = now();
        LocalDateTime closeDate1 = multiply(publishDate1, 3);
        String storageCode = draftDataService.applyDraft(null, draftCode, publishDate1, closeDate1);

        draftCode = draftDataService.createDraft(fields);

        rows.get(1)
                .getFieldValue(code.getName())
                .setValue(null);
        rows.get(2)
                .getFieldValue(code.getName())
                .setValue("003_1");
        rows.get(5)
                .getFieldValue(code.getName())
                .setValue("004_1");
        draftDataService.addRows(draftCode, rows.subList(1, 7));
        LocalDateTime publishDate2 = multiply(publishDate1, 2);
        LocalDateTime closeDate2 = closeDate1;
        String storageCode2 = draftDataService.applyDraft(storageCode, draftCode, publishDate2, closeDate2);

        Set<List<FieldSearchCriteria>> fieldValues = rows
                .stream()
                .map(row -> singletonList(
                        new FieldSearchCriteria(id, SearchTypeEnum.EXACT, singletonList(row.getFieldValue(id.getName()).getValue()))
                        )
                )
                .collect(Collectors.toSet());

        // WARN: bug? : replace first publishDate2 with closeDate1
        CompareDataCriteria compareDataCriteria = new CompareDataCriteria(storageCode2, null, publishDate1, publishDate2, publishDate2, closeDate2, fields, singletonList(FIELD_ID_CODE), fieldValues);
        DataDifference actualDataDifference = compareDataService.getDataDifference(compareDataCriteria);
        List<DiffRowValue> expectedDiffRowValues = new ArrayList<>();
        expectedDiffRowValues.add(new DiffRowValue(
                fields.stream()
                        .map(field -> new DiffFieldValue<>(field, rows.get(0).getFieldValue(field.getName()).getValue(), null, DiffStatusEnum.DELETED))
                        .collect(toList()),
                DiffStatusEnum.DELETED));
        expectedDiffRowValues.add(new DiffRowValue(
                fields.stream()
                        .map(field -> new DiffFieldValue<>(field,
                                field.equals(code) ? "002" : null,
                                field.equals(id) ? BigInteger.valueOf(2) : null,
                                field.equals(code) ? DiffStatusEnum.UPDATED : null))
                        .collect(toList()),
                DiffStatusEnum.UPDATED));
        expectedDiffRowValues.add(new DiffRowValue(
                fields.stream()
                        .map(field -> new DiffFieldValue<>(field,
                                field.equals(code) ? "003" : null,
                                rows.get(2).getFieldValue(field.getName()).getValue(),
                                field.equals(code) ? DiffStatusEnum.UPDATED : null))
                        .collect(toList()),
                DiffStatusEnum.UPDATED));
        expectedDiffRowValues.add(new DiffRowValue(
                fields.stream()
                        .map(field -> new DiffFieldValue<>(field,
                                null,
                                rows.get(5).getFieldValue(field.getName()).getValue(),
                                field.equals(code) ? DiffStatusEnum.UPDATED : null))
                        .collect(toList()),
                DiffStatusEnum.UPDATED));
        expectedDiffRowValues.add(new DiffRowValue(
                fields.stream()
                        .map(field -> new DiffFieldValue<>(field,
                                null,
                                rows.get(6).getFieldValue(field.getName()).getValue(),
                                DiffStatusEnum.INSERTED))
                        .collect(toList()),
                DiffStatusEnum.INSERTED));
        assertDiffRowValues(expectedDiffRowValues, (List<DiffRowValue>) actualDataDifference.getRows().getCollection());
    }

    /*
     * testing get data difference between two published versions when composite primary key and existing common non-primary fields
     */
    @Test
    public void testGetDataDifferenceForTwoPublishedVersionsWithCompositePK() {
        List<Field> fields = new ArrayList<>();
        Field id = fieldFactory.createField(FIELD_ID_CODE, FieldType.INTEGER);
        Field code = fieldFactory.createField(FIELD_CODE_CODE, FieldType.STRING);
        Field name = fieldFactory.createField(FIELD_NAME_CODE, FieldType.STRING);
        fields.add(id);
        fields.add(code);
        fields.add(name);
        List<RowValue> rows = new ArrayList<>();
        rows.add(new LongRowValue(
                id.valueOf(BigInteger.valueOf(1)),
                code.valueOf("001"),
                name.valueOf("name1")));
        rows.add(new LongRowValue(
                id.valueOf(BigInteger.valueOf(2)),
                code.valueOf("002"),
                name.valueOf("name2")));
        rows.add(new LongRowValue(
                id.valueOf(BigInteger.valueOf(3)),
                code.valueOf("003"),
                name.valueOf("name3")));
        rows.add(new LongRowValue(
                id.valueOf(BigInteger.valueOf(4)),
                code.valueOf("004"),
                name.valueOf("name4")));

        String draftCode = draftDataService.createDraft(fields);
        draftDataService.addRows(draftCode, asList(rows.get(0), rows.get(1), rows.get(2)));
        LocalDateTime publishDate1 = now();
        LocalDateTime closeDate1 = multiply(publishDate1, 3);
        String storageCode = draftDataService.applyDraft(null, draftCode, publishDate1, closeDate1);

        draftCode = draftDataService.createDraft(fields);

        rows.get(2).getFieldValue(name.getName()).setValue("name3_1");
        draftDataService.addRows(draftCode, asList(rows.get(1), rows.get(2), rows.get(3)));
        LocalDateTime publishDate2 = multiply(publishDate1, 2);
        LocalDateTime closeDate2 = closeDate1;
        String storageCode2 = draftDataService.applyDraft(storageCode, draftCode, publishDate2, closeDate2);

        Set<List<FieldSearchCriteria>> fieldValues = rows
                .stream()
                .map(row -> asList(
                        new FieldSearchCriteria(id, SearchTypeEnum.EXACT, singletonList(row.getFieldValue(id.getName()).getValue())),
                        new FieldSearchCriteria(code, SearchTypeEnum.EXACT, singletonList(row.getFieldValue(code.getName()).getValue()))
                        )
                )
                .collect(Collectors.toSet());

        // WARN: bug? : replace first publishDate2 with closeDate1
        CompareDataCriteria compareDataCriteria = new CompareDataCriteria(storageCode2, null, publishDate1, publishDate2, publishDate2, closeDate2, fields, Arrays.asList(FIELD_ID_CODE, FIELD_CODE_CODE), fieldValues);
        DataDifference actualDataDifference = compareDataService.getDataDifference(compareDataCriteria);
        List<DiffRowValue> expectedDiffRowValues = new ArrayList<>();
        expectedDiffRowValues.add(new DiffRowValue(
                fields.stream()
                        .map(field -> new DiffFieldValue<>(field, rows.get(0).getFieldValue(field.getName()).getValue(), null, DiffStatusEnum.DELETED))
                        .collect(toList()),
                DiffStatusEnum.DELETED));
        expectedDiffRowValues.add(new DiffRowValue(
                fields.stream()
                        .map(field -> new DiffFieldValue<>(field, field.equals(name) ? "name3" : null, rows.get(2).getFieldValue(field.getName()).getValue(), field.equals(name) ? DiffStatusEnum.UPDATED : null))
                        .collect(toList()),
                DiffStatusEnum.UPDATED));
        expectedDiffRowValues.add(new DiffRowValue(
                fields.stream()
                        .map(field -> new DiffFieldValue<>(field, null, rows.get(3).getFieldValue(field.getName()).getValue(), DiffStatusEnum.INSERTED))
                        .collect(toList()),
                DiffStatusEnum.INSERTED));
        assertDiffRowValues(expectedDiffRowValues, (List<DiffRowValue>) actualDataDifference.getRows().getCollection());
    }

    /*
     * testing get data difference between two published versions when composite primary key and NO common non-primary fields
     * there must be 0 UPDATED rows, just deleted and inserted
     */
    @Test
    public void testGetDataDifferenceForTwoPublishedVersionsWithoutNonPrimaryFields() {
        List<Field> fields = new ArrayList<>();
        Field id = fieldFactory.createField(FIELD_ID_CODE, FieldType.INTEGER);
        Field code = fieldFactory.createField(FIELD_CODE_CODE, FieldType.STRING);
        fields.add(id);
        fields.add(code);
        List<RowValue> rows = new ArrayList<>();
        rows.add(new LongRowValue(
                id.valueOf(BigInteger.valueOf(1)),
                code.valueOf("001")));
        rows.add(new LongRowValue(
                id.valueOf(BigInteger.valueOf(2)),
                code.valueOf("002")));
        rows.add(new LongRowValue(
                id.valueOf(BigInteger.valueOf(3)),
                code.valueOf("003")));
        rows.add(new LongRowValue(
                id.valueOf(BigInteger.valueOf(4)),
                code.valueOf("004")));

        String draftCode = draftDataService.createDraft(fields);
        draftDataService.addRows(draftCode, asList(rows.get(0), rows.get(1), rows.get(2)));
        LocalDateTime publishDate1 = now();
        LocalDateTime closeDate1 = multiply(publishDate1, 3);
        String storageCode = draftDataService.applyDraft(null, draftCode, publishDate1, closeDate1);

        draftCode = draftDataService.createDraft(fields);

        draftDataService.addRows(draftCode, asList(rows.get(1), rows.get(2), rows.get(3)));
        LocalDateTime publishDate2 = multiply(publishDate1, 2);
        LocalDateTime closeDate2 = closeDate1;
        String storageCode2 = draftDataService.applyDraft(storageCode, draftCode, publishDate2, closeDate2);

        Set<List<FieldSearchCriteria>> fieldValues = rows
                .stream()
                .map(row -> asList(
                        new FieldSearchCriteria(id, SearchTypeEnum.EXACT, singletonList(row.getFieldValue(id.getName()).getValue())),
                        new FieldSearchCriteria(code, SearchTypeEnum.EXACT, singletonList(row.getFieldValue(code.getName()).getValue()))
                        )
                )
                .collect(Collectors.toSet());

        // WARN: bug? : replace first publishDate2 with closeDate1
        CompareDataCriteria compareDataCriteria = new CompareDataCriteria(storageCode2, null, publishDate1, publishDate2, publishDate2, closeDate2, fields, Arrays.asList(FIELD_ID_CODE, FIELD_CODE_CODE), fieldValues);
        DataDifference actualDataDifference = compareDataService.getDataDifference(compareDataCriteria);
        List<DiffRowValue> expectedDiffRowValues = new ArrayList<>();
        expectedDiffRowValues.add(new DiffRowValue(
                fields.stream()
                        .map(field -> new DiffFieldValue<>(field, rows.get(0).getFieldValue(field.getName()).getValue(), null, DiffStatusEnum.DELETED))
                        .collect(toList()),
                DiffStatusEnum.DELETED));
        expectedDiffRowValues.add(new DiffRowValue(
                fields.stream()
                        .map(field -> new DiffFieldValue<>(field, null, rows.get(3).getFieldValue(field.getName()).getValue(), DiffStatusEnum.INSERTED))
                        .collect(toList()),
                DiffStatusEnum.INSERTED));
        assertDiffRowValues(expectedDiffRowValues, (List<DiffRowValue>) actualDataDifference.getRows().getCollection());
    }

    /*
     * testing get data difference between published version and draft
     */
    @Test
    public void testGetDataDifferenceForPublishedAndDraft() {
        List<Field> fields = new ArrayList<>();
        Field id = fieldFactory.createField(FIELD_ID_CODE, FieldType.INTEGER);
        Field code = fieldFactory.createField(FIELD_CODE_CODE, FieldType.STRING);
        fields.add(id);
        fields.add(code);
        List<RowValue> rows = new ArrayList<>();
        rows.add(new LongRowValue(
                id.valueOf(BigInteger.valueOf(1)),
                code.valueOf("001")));
        rows.add(new LongRowValue(
                id.valueOf(BigInteger.valueOf(2)),
                code.valueOf("002")));
        rows.add(new LongRowValue(
                id.valueOf(BigInteger.valueOf(3)),
                code.valueOf("003")));
        rows.add(new LongRowValue(
                id.valueOf(BigInteger.valueOf(4)),
                code.valueOf("004")));

        String draftCode = draftDataService.createDraft(fields);
        draftDataService.addRows(draftCode, asList(rows.get(0), rows.get(1), rows.get(2)));
        LocalDateTime publishDate1 = now();
        LocalDateTime closeDate1 = multiply(publishDate1, 3);
        String storageCode = draftDataService.applyDraft(null, draftCode, publishDate1, closeDate1);

        draftCode = draftDataService.createDraft(fields);

        rows.get(2).getFieldValue(code.getName()).setValue("003_1");
        draftDataService.addRows(draftCode, asList(rows.get(1), rows.get(2), rows.get(3)));

        Set<List<FieldSearchCriteria>> fieldValues = rows
                .stream()
                .map(row -> singletonList(
                        new FieldSearchCriteria(id, SearchTypeEnum.EXACT, singletonList(row.getFieldValue(id.getName()).getValue()))
                        )
                )
                .collect(Collectors.toSet());

        CompareDataCriteria compareDataCriteria = new CompareDataCriteria(storageCode, draftCode, publishDate1, closeDate1, null, null, fields, singletonList(FIELD_ID_CODE), fieldValues);
        DataDifference actualDataDifference = compareDataService.getDataDifference(compareDataCriteria);
        List<DiffRowValue> expectedDiffRowValues = new ArrayList<>();
        expectedDiffRowValues.add(new DiffRowValue(
                fields.stream()
                        .map(field -> new DiffFieldValue<>(field, rows.get(0).getFieldValue(field.getName()).getValue(), null, DiffStatusEnum.DELETED))
                        .collect(toList()),
                DiffStatusEnum.DELETED));
        expectedDiffRowValues.add(new DiffRowValue(
                fields.stream()
                        .map(field -> new DiffFieldValue<>(field, field.equals(code) ? "003" : null, rows.get(2).getFieldValue(field.getName()).getValue(), field.equals(code) ? DiffStatusEnum.UPDATED : null))
                        .collect(toList()),
                DiffStatusEnum.UPDATED));
        expectedDiffRowValues.add(new DiffRowValue(
                fields.stream()
                        .map(field -> new DiffFieldValue<>(field, null, rows.get(3).getFieldValue(field.getName()).getValue(), DiffStatusEnum.INSERTED))
                        .collect(toList()),
                DiffStatusEnum.INSERTED));
        assertDiffRowValues(expectedDiffRowValues, (List<DiffRowValue>) actualDataDifference.getRows().getCollection());
    }

    /*
     * testing get data difference between two published versions when no diff
     * there must be 0 rows
     */
    @Test
    public void testGetDataDifferenceWhenNoDiff() {
        LocalDateTime publishDate1 = now();
        LocalDateTime closeDate1 = multiply(publishDate1, 2);
        LocalDateTime publishDate2 = multiply(publishDate1, 3);

        List<Field> fields = new ArrayList<>();
        Field id = fieldFactory.createField(FIELD_ID_CODE, FieldType.INTEGER);
        Field code = fieldFactory.createField(FIELD_CODE_CODE, FieldType.STRING);
        Field name = fieldFactory.createField(FIELD_NAME_CODE, FieldType.STRING);
        fields.add(id);
        fields.add(code);

        List<RowValue> rows = new ArrayList<>();
        rows.add(new LongRowValue(
                id.valueOf(BigInteger.valueOf(1)),
                code.valueOf("001")));
        rows.add(new LongRowValue(
                id.valueOf(BigInteger.valueOf(2)),
                code.valueOf("002")));
        rows.add(new LongRowValue(
                id.valueOf(BigInteger.valueOf(3)),
                code.valueOf("003")));
        rows.add(new LongRowValue(
                id.valueOf(BigInteger.valueOf(4)),
                code.valueOf("004")));

        String draftCode = draftDataService.createDraft(fields);
        draftDataService.addRows(draftCode, rows);
        String storageCode = draftDataService.applyDraft(null, draftCode, publishDate1, closeDate1);

        fields.add(name);
        draftCode = draftDataService.createDraft(fields);
        draftDataService.addRows(draftCode, rows);
        String storageCode2 = draftDataService.applyDraft(storageCode, draftCode, publishDate2, null);

        Set<List<FieldSearchCriteria>> fieldValues = rows
                .stream()
                .map(row -> asList(
                        new FieldSearchCriteria(id, SearchTypeEnum.EXACT, singletonList(row.getFieldValue(id.getName()).getValue())),
                        new FieldSearchCriteria(code, SearchTypeEnum.EXACT, singletonList(row.getFieldValue(code.getName()).getValue()))
                        )
                )
                .collect(Collectors.toSet());

        CompareDataCriteria compareDataCriteria = new CompareDataCriteria(storageCode, storageCode2, publishDate1, closeDate1, publishDate2, null, Arrays.asList(fields.get(0), fields.get(1)), singletonList(FIELD_ID_CODE), fieldValues);
        DataDifference actualDataDifference = compareDataService.getDataDifference(compareDataCriteria);
        assertDiffRowValues(new ArrayList<>(), (List<DiffRowValue>) actualDataDifference.getRows().getCollection());
    }

    private void assertDiffRowValues(List<DiffRowValue> expectedDiffRowValues, List<DiffRowValue> actualDiffRowValues) {
        assertEquals(expectedDiffRowValues.size(), actualDiffRowValues.size());
        expectedDiffRowValues.forEach(expectedDiffRowValue -> {
            if (actualDiffRowValues.stream().noneMatch(actualDiffRowValue ->
                    expectedDiffRowValue.getValues().size() == actualDiffRowValue.getValues().size() && actualDiffRowValue.getValues().containsAll(expectedDiffRowValue.getValues())))
                fail();
        });
    }

    /**
     * При поиске, когда нет даты закрытия, не учитывалась правая граница.
     * <a href="https://jira.i-novus.ru/browse/RDM-130">Ссылка на баг.</a>
     */
    @Test
    public void testPublishAndSearchNullCloseDateData() {
        Field stringField = new StringField("string");
        String draftStorageCode1 = draftDataService.createDraft(singletonList(stringField));
        String draftStorageCode2 = draftDataService.createDraft(singletonList(stringField));
        String draftStorageCode3 = draftDataService.createDraft(singletonList(stringField));
        List<RowValue> rowValues1 = singletonList(
                new LongRowValue(stringField.valueOf("string field value 1")));
        List<RowValue> rowValues2 = Arrays.asList(
                new LongRowValue(stringField.valueOf("string field value 1")),
                new LongRowValue(stringField.valueOf("string field value 2.1")),
                new LongRowValue(stringField.valueOf("string field value 2.2")));
        List<RowValue> rowValues3 = singletonList(
                new LongRowValue(stringField.valueOf("string field value 3")));
        draftDataService.addRows(draftStorageCode1, rowValues1);
        draftDataService.addRows(draftStorageCode2, rowValues2);
        draftDataService.addRows(draftStorageCode3, rowValues3);

        //Публикация двух версий с определенной датой закрытия
        LocalDateTime publishDate1 = now();
        LocalDateTime closeDate1 = publishDate1.plusSeconds(60 * 60 * 24);
        String versionStorageCode1 = draftDataService.applyDraft(null, draftStorageCode1, publishDate1, closeDate1);

        LocalDateTime publishDate2 = closeDate1;
        LocalDateTime closeDate2 = publishDate2.plusSeconds(60 * 60 * 24);
        String versionStorageCode2 = draftDataService.applyDraft(versionStorageCode1, draftStorageCode2, publishDate2, closeDate2);

        //Поиск когда closeTime поиска и данных closeTime одинаковый
        StorageDataCriteria dataCriteria = new StorageDataCriteria(versionStorageCode2, publishDate2, closeDate2, singletonList(stringField), emptySet(), null);
        CollectionPage<RowValue> actualData = searchDataService.getPagedData(dataCriteria);
        assertEquals(3, actualData.getCount());
        assertRows(rowValues2, actualData.getCollection());

        //Поиск когда closeTime поиска null а у данных определен
        //Ожидается пустой ответ, т.к. данные не действуют на всем промежутке
        dataCriteria = new StorageDataCriteria(versionStorageCode2, publishDate2, null, singletonList(stringField), emptySet(), null);
        actualData = searchDataService.getPagedData(dataCriteria);
        assertEquals(0, actualData.getCount());
        assertTrue(actualData.getCollection().isEmpty());

        LocalDateTime publishDate3 = closeDate1;
        String versionStorageCode3 = draftDataService.applyDraft(versionStorageCode2, draftStorageCode3, publishDate3, null);

        //Поиск когда closeTime поиска определен а у данных нет
        //Ожидается одна строка (Последняя опубликованная)
        dataCriteria = new StorageDataCriteria(versionStorageCode3, publishDate3, null, singletonList(stringField), emptySet(), null);
        actualData = searchDataService.getPagedData(dataCriteria);
        assertEquals(1, actualData.getCount());
        assertRows(rowValues3, actualData.getCollection());
    }

    @Test
    public void testGroupFilters() {
        String id = "id";
        List<Field> fields = new ArrayList<>();
        Field integerField = new IntegerField(id);
        fields.add(integerField);
        String storageCode = draftDataService.createDraft(fields);
        RowValue[] records = {
            new LongRowValue(new IntegerFieldValue(id, 1)),
            new LongRowValue(new IntegerFieldValue(id, 2)),
            new LongRowValue(new IntegerFieldValue(id, 3)),
            new LongRowValue(new IntegerFieldValue(id, 4)),
            new LongRowValue(new IntegerFieldValue(id, 5)),
            new LongRowValue(new IntegerFieldValue(id, 6))
        };
        draftDataService.addRows(storageCode, Arrays.asList(records));

//      Имитируем поведение недалекого клиента, который хочет найти строки, у которых id равен либо 1, либо 2, либо 3.
//      Для этого он, вместо того, чтобы создать new FieldSearchCriteria(integerField, SearchTypeEnum.EXACT, Arrays.asList(1, 2, 3),
//      Создаст 3 разных FieldSearchCriteria, в которые аргументом values укажем синглтон лист со значениями 1, 2 и 3.
//      Если мы не сгруппируем 3 данные FieldSearchCriteria в одну FieldSearchCriteria, наш генератор SQL-запросов составит запрос таким образом:
//      "SELECT ... FROM ... WHERE 1 = 1 AND ("id" IN (1) AND "id" IN (2) AND "id" IN (3))". Последнее подвыражение никогда не будет правдой ни для какой записи.
//      Пример не высосан из пальца и как минимум в RDM группировки не производятся.
        FieldSearchCriteria criteria1 = new FieldSearchCriteria(integerField, SearchTypeEnum.EXACT, singletonList(1));
        FieldSearchCriteria criteria2 = new FieldSearchCriteria(integerField, SearchTypeEnum.EXACT, singletonList(2));
        FieldSearchCriteria criteria3 = new FieldSearchCriteria(integerField, SearchTypeEnum.EXACT, singletonList(3));

        StorageDataCriteria dataCriteria = new StorageDataCriteria(storageCode, null, null, fields, Arrays.asList(criteria1, criteria2, criteria3), null);
        CollectionPage<RowValue> data = searchDataService.getPagedData(dataCriteria);
        assertEquals(3, data.getCount());

        Collection<RowValue> collection = data.getCollection();
        BigInteger[] searchIds = {BigInteger.valueOf(1), BigInteger.valueOf(2), BigInteger.valueOf(3)};
        for (RowValue rowValue : collection) {
            assertThat(Arrays.asList(searchIds), hasItem(rowValue.getFieldValue(id).getValue()));
        }
    }

}