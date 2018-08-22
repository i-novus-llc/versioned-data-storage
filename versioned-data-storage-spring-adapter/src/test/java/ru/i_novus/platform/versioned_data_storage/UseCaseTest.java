package ru.i_novus.platform.versioned_data_storage;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import ru.i_novus.platform.datastorage.temporal.enums.FieldType;
import ru.i_novus.platform.datastorage.temporal.model.Field;
import ru.i_novus.platform.datastorage.temporal.model.FieldValue;
import ru.i_novus.platform.datastorage.temporal.model.LongRowValue;
import ru.i_novus.platform.datastorage.temporal.model.Reference;
import ru.i_novus.platform.datastorage.temporal.model.criteria.DataCriteria;
import ru.i_novus.platform.datastorage.temporal.model.criteria.FieldSearchCriteria;
import ru.i_novus.platform.datastorage.temporal.model.criteria.SearchTypeEnum;
import ru.i_novus.platform.datastorage.temporal.model.value.ReferenceFieldValue;
import ru.i_novus.platform.datastorage.temporal.model.value.RowValue;
import ru.i_novus.platform.datastorage.temporal.service.DraftDataService;
import ru.i_novus.platform.datastorage.temporal.service.FieldFactory;
import ru.i_novus.platform.datastorage.temporal.service.SearchDataService;
import ru.i_novus.platform.versioned_data_storage.config.VersionedDataStorageConfig;
import ru.i_novus.platform.versioned_data_storage.pg_impl.service.FieldFactoryImpl;

import javax.persistence.PersistenceException;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

/**
 * Created by tnurdinov on 08.06.2018.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {JpaTestConfig.class, VersionedDataStorageConfig.class})
public class UseCaseTest {

    private static final Logger logger = LoggerFactory.getLogger(UseCaseTest.class);

    private static final String SYS_PUBLISHTIME = "SYS_PUBLISHTIME";
    private static final String SYS_CLOSETIME = "SYS_CLOSETIME";

    @Autowired
    private DraftDataService draftDataService;

    @Autowired
    private SearchDataService searchDataService;

    private FieldFactory fieldFactory = new FieldFactoryImpl();


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
        Field d_a_id = fieldFactory.createField("ID", FieldType.INTEGER);
        Field d_a_name = fieldFactory.createField("NAME", FieldType.STRING);
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


        DataCriteria criteria = new DataCriteria(d_a_draftCode, null, null, d_a_fields, null, null);
        Collection<RowValue> d_a_actualRows = searchDataService.getPagedData(criteria).getCollection();
        assertRows(d_a_rows, d_a_actualRows);
        logger.info("<<<<<<<<<<<<<<< 1 этап завершен >>>>>>>>>>>>>>>>>>>>>");

        logger.info("<<<<<<<<<<<<<<< 2 этап >>>>>>>>>>>>>>>>>>>>>");
        Date s_a_publishTime = new Date();
        Date beforeSAPublishDate = Date.from(s_a_publishTime.toInstant().minus(1, ChronoUnit.DAYS));
        String s_a_storageCode = draftDataService.applyDraft(null, d_a_draftCode, s_a_publishTime);
        Collection<RowValue> s_a_actualRows = searchDataService.getData(new DataCriteria(s_a_storageCode, beforeSAPublishDate, null, d_a_fields, null, null));
        assertEquals(0, s_a_actualRows.size());
        s_a_actualRows = searchDataService.getData(new DataCriteria(s_a_storageCode, s_a_publishTime, null, d_a_fields, null, null));
        assertRows(d_a_rows, s_a_actualRows);
        logger.info("<<<<<<<<<<<<<<< 2 этап завершен >>>>>>>>>>>>>>>>>>>>>");

        logger.info("<<<<<<<<<<<<<<< 3 этап >>>>>>>>>>>>>>>>>>>>>");
        Field d_b_id = fieldFactory.createField("ID", FieldType.INTEGER);
        Field d_b_name = fieldFactory.createField("NAME", FieldType.STRING);
        Field d_b_ref = fieldFactory.createField("REF", FieldType.REFERENCE);
        List<Field> d_b_fields = Arrays.asList(d_b_id, d_b_name, d_b_ref);
        String d_b_draftCode = draftDataService.createDraft(d_b_fields);
        RowValue d_b_rowValue = new LongRowValue(
                d_b_id.valueOf(BigInteger.valueOf(1)),
                d_b_name.valueOf("name"),
                d_b_ref.valueOf(new Reference(s_a_storageCode, s_a_publishTime, d_a_id.getName(), d_a_name.getName(), "1", "test"))
        );
        draftDataService.addRows(d_b_draftCode, Arrays.asList(d_b_rowValue));
        List<RowValue> d_b_actualRows = searchDataService.getData(new DataCriteria(d_b_draftCode, null, null, d_b_fields, null, "name"));
        Object systemId = d_b_actualRows.get(0).getSystemId();

        d_b_rowValue = new LongRowValue(
                d_b_id.valueOf(BigInteger.valueOf(1)),
                d_b_name.valueOf("name"),
                d_b_ref.valueOf(new Reference(s_a_storageCode, s_a_publishTime, d_a_id.getName(), d_a_name.getName(), "2"))
        );
        d_b_rowValue.setSystemId(systemId);
        draftDataService.updateRow(d_b_draftCode, d_b_rowValue);
        d_b_actualRows = searchDataService.getData(new DataCriteria(d_b_draftCode, null, null, d_b_fields, null, "name"));
        d_b_rowValue.getFieldValues().forEach(value -> {
            if (value instanceof ReferenceFieldValue) {
                ((ReferenceFieldValue) value).getValue().setDisplayValue("test2");
            }
        });
        assertRows(Arrays.asList(d_b_rowValue), d_b_actualRows);
        logger.info("<<<<<<<<<<<<<<< 3 этап завершен >>>>>>>>>>>>>>>>>>>>>");


        logger.info("<<<<<<<<<<<<<<< 4 этап >>>>>>>>>>>>>>>>>>>>>");
        Date s_b_publishTime = new Date();
        String s_b_storageCode = draftDataService.applyDraft(null, d_b_draftCode, s_b_publishTime);
        Collection<RowValue> s_b_actualRows = searchDataService.getData(new DataCriteria(s_b_storageCode, null, null, d_b_fields, null, null));
        assertRows(Arrays.asList(d_b_rowValue), s_b_actualRows);
        logger.info("<<<<<<<<<<<<<<< 4 этап завершен >>>>>>>>>>>>>>>>>>>>>");

    }

    @Test
    public void testUpdateDraft() {
        String existingStorageCode = createStorage();
        List<Field> fields = new ArrayList<>();
        Field code = fieldFactory.createField("CODE", FieldType.STRING);
        Field name = fieldFactory.createField("NAME", FieldType.STRING);
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
        List<RowValue> rows = new ArrayList<>();
        RowValue rowValue = new LongRowValue(
                code.valueOf("001"),
                name.valueOf("name"),
                ref_new.valueOf(new Reference(existingStorageCode, new Date(), "ID", "NAME", "1", "test")),
                date_col.valueOf(LocalDate.now()));
        rows.add(rowValue);
        draftDataService.addRows(draftCode, rows);
        List<RowValue> actualRows = searchDataService.getData(new DataCriteria(draftCode, null, null, fields, null, null));
        assertRows(rows, actualRows);
        String storageCode = draftDataService.applyDraft(null, draftCode, new Date());
        actualRows = searchDataService.getData(new DataCriteria(storageCode, null, null, fields, null, null));
        assertRows(rows, actualRows);
    }

    private void assertRows(List<RowValue> expectedRows, Collection<RowValue> actualRows) {
        assertEquals("result size not equals", expectedRows.size(), actualRows.size());
        Assert.assertTrue(
                "not equals actualRows: \n" + actualRows.stream().map(RowValue::toString).collect(Collectors.joining(", ")) + " \n and expected rows: \n" + expectedRows.stream().map(RowValue::toString).collect(Collectors.joining(", "))
                , actualRows.stream().filter(actualRow -> expectedRows.stream().filter(expectedRow -> equalsFieldValues(expectedRow.getFieldValues(), actualRow.getFieldValues())).findAny().isPresent()).findAny().isPresent());
    }

    private boolean equalsFieldValues(List<FieldValue> values1, List<FieldValue> values2) {
        if (values1 == values2)
            return true;
        if (values1 == null || values2 == null || values1.size() != values2.size())
            return false;

        for (FieldValue val1 : values1) {
            boolean isPresent = values2.stream().filter(val2 -> {
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

            }).findAny().isPresent();
            if (!isPresent) {
                return false;
            }

        }

        return true;

    }

    private String createStorage() {
        List<Field> d_a_fields = new ArrayList<>();
        Field d_a_id = fieldFactory.createField("ID", FieldType.INTEGER);
        Field d_a_name = fieldFactory.createField("NAME", FieldType.STRING);
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
        return draftDataService.applyDraft(null, d_a_draftCode, new Date());
    }

    /**
     * Запись двух одинаковых строк в один черновик
     * Ожидается ошибка БД - нарушение уникальности "SYS_HASH"(код 23505)
     */
    @Test
    public void testCreateUniqueHash() throws Exception{
        List<Field> d_a_fields = new ArrayList<>();
        Field d_a_id = fieldFactory.createField("ID", FieldType.INTEGER);
        Field d_a_name = fieldFactory.createField("NAME", FieldType.STRING);
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
        } catch (PersistenceException e) {
            if (!(e.getCause().getCause() instanceof PSQLException &&
                    "23505".equals(((PSQLException)e.getCause().getCause()).getSQLState()))){
                Assert.fail("Two equals row error");
            }
        }
        draftDataService.applyDraft(null, d_a_draftCode, new Date());
    }

    /**
     * Удаление поля из черновика
     * Ожидается: обновление SYS_HASH и FTS у существующих записей
     */
    @Test
    public void testDeleteField() {
        List<Field> fields = new ArrayList<>();
        Field id = fieldFactory.createField("ID", FieldType.INTEGER);
        Field name = fieldFactory.createField("NAME", FieldType.STRING);
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

        List<Field> hashField = singletonList(fieldFactory.createField("SYS_HASH", FieldType.STRING));
        DataCriteria criteria = new DataCriteria(storageCode, null, null, hashField, null, null);

        RowValue hashBeforeDelete = searchDataService.getData(criteria).get(0);
        draftDataService.deleteField(storageCode, "NAME");
        RowValue dataAfterDelete = searchDataService.getData(criteria).get(0);

        Assert.assertNotEquals(hashBeforeDelete, dataAfterDelete);

        fields.remove(name);
        List<RowValue> searchDeletedField = searchDataService.getData(new DataCriteria(storageCode, null, null, fields, null, "test"));
        List<RowValue> searchExistingField = searchDataService.getData(new DataCriteria(storageCode, null, null, fields, null, "1"));

        Assert.assertTrue(searchDeletedField.isEmpty());
        Assert.assertTrue(!searchExistingField.isEmpty());

    }

    /**
     * Добавление поля из черновика
     * Ожидается: обновление SYS_HASH у существующих записей
     */
    @Test
    public void testAddField() {
        List<Field> fields = new ArrayList<>();
        Field id = fieldFactory.createField("ID", FieldType.INTEGER);
        Field name = fieldFactory.createField("NAME", FieldType.STRING);
        fields.add(id);
        fields.add(name);

        String storageCode = draftDataService.createDraft(fields);
        List<RowValue> rows = singletonList(new LongRowValue(
                id.valueOf(BigInteger.valueOf(1)),
                name.valueOf("test")));
        draftDataService.addRows(storageCode, rows);

        List<Field> hashField = singletonList(fieldFactory.createField("SYS_HASH", FieldType.STRING));
        DataCriteria criteria = new DataCriteria(storageCode, null, null, hashField, null, null);

        RowValue hashBeforeAdd = searchDataService.getData(criteria).get(0);
        Field date = fieldFactory.createField("DATE", FieldType.DATE);
        draftDataService.addField(storageCode, date);
        RowValue dataAfterAdd = searchDataService.getData(criteria).get(0);

        Assert.assertNotEquals(hashBeforeAdd, dataAfterAdd);
    }

    /*
     * testing manipulations while publishing a draft with closeDate
     */
    @Test
    public void testApplyDraftWithCloseDate() {
        Long time_long_diff = 150000000L;

        List<Field> fields = new ArrayList<>();
        Field id = fieldFactory.createField("ID", FieldType.INTEGER);
        Field code = fieldFactory.createField("CODE", FieldType.STRING);
        Field name = fieldFactory.createField("NAME", FieldType.STRING);
        fields.add(id);
        fields.add(code);
        fields.add(name);
        final List<Field> withSysDateFields = Arrays.asList(id, code, name, fieldFactory.createField(SYS_PUBLISHTIME, FieldType.STRING), fieldFactory.createField(SYS_CLOSETIME, FieldType.STRING));
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
        Date publishDate1 = new Date();
        Date closeDate1 = new Date(publishDate1.getTime() + time_long_diff);
        String storageCode = draftDataService.applyDraft(null, draftCode, publishDate1, closeDate1);
        List<RowValue> actualRows = searchDataService.getData(new DataCriteria(storageCode, publishDate1, closeDate1, fields, null, null));
//        проверка в опубликованной версии
        assertRows(rows1, actualRows);
        // "1" (pT1, cT1), "2" (pT1, cT1) общее кол-во записей в таблице версий
        assertEquals(2, searchDataService.getData(new DataCriteria(storageCode, null, null, fields, null, null)).size());

        // pT1 < pT2 < cT2 < cT1
        draftCode = draftDataService.createDraft(fields);
        List<RowValue> rows2 = new ArrayList<>();
        rows2.add(rows.get(1));
        rows2.add(rows.get(2));
        //удаляем rows.get(0)
        draftDataService.addRows(draftCode, rows2);
        Date publishDate2 = new Date(publishDate1.getTime() + (int) (time_long_diff * 0.01));
        Date closeDate2 = new Date(publishDate1.getTime() + (int) (time_long_diff * 0.5));
        String storageCode2 = draftDataService.applyDraft(storageCode, draftCode, publishDate2, closeDate2);
        actualRows = searchDataService.getData(new DataCriteria(storageCode2, publishDate2, closeDate2, fields, null, null));
        assertRows(rows2, actualRows);
        System.out.println("publishDate2 = " + publishDate2 + "\n closeDate2 = " + closeDate2 + "" +
                "\npublishDate1 = " + publishDate1 + "\n closeDate1 = " + closeDate1 + "" +
                " \n rows = " + searchDataService.getData(new DataCriteria(storageCode2, null, null, withSysDateFields, null, null)).toString());
        // "1" (pT1, pT2), "1" (cT2, cT1), "2" (pT1, cT1), "3" (pT2, cT2)
        assertEquals("incorrect total rows count in version table", 4, searchDataService.getData(new DataCriteria(storageCode2, null, null, fields, null, null)).size());
        // pT1 < pT2 < pT3 < cT3 < cT2 < cT1
        draftCode = draftDataService.createDraft(fields);
        List<RowValue> rows3 = new ArrayList<>();
        rows3.add(rows.get(0));
        draftDataService.addRows(draftCode, rows3);
        Date publishDate3 = new Date(publishDate1.getTime() + (int) (time_long_diff * 0.1));
        Date closeDate3 = new Date(publishDate1.getTime() + (int) (time_long_diff * 0.4));
        String storageCode3 = draftDataService.applyDraft(storageCode2, draftCode, publishDate3, closeDate3);
        actualRows = searchDataService.getData(new DataCriteria(storageCode3, publishDate3, closeDate3, fields, null, null));
        System.out.println("publishDate3 = "+ publishDate3 + "\ncloseDate3 = " + closeDate3 + " \n" + searchDataService.getData(new DataCriteria(storageCode3, publishDate3, closeDate3, withSysDateFields, null, null)));
        assertRows(rows3, actualRows);
        // "1" (pT1, pT2), "1" (pT3, cT3), "1" (cT2, cT1), "2" (pT1, pT3), "2" (cT3, cT1), "3" (pT2, pT3), "3" (cT3, cT2)
        assertEquals(7, searchDataService.getData(new DataCriteria(storageCode3, null, null, fields, null, null)).size());

        // pT1 < pT2 <= pT4 < cT4 <= pT3 < cT3 < cT2 < cT1
        draftCode = draftDataService.createDraft(fields);
        List<RowValue> rows4 = new ArrayList<>(rows);
        draftDataService.addRows(draftCode, rows4);
        Date publishDate4 = new Date(publishDate1.getTime() + (int) (time_long_diff * 0.01));
        Date closeDate4 = new Date(publishDate1.getTime() + (int) (time_long_diff * 0.1));
        String storageCode4 = draftDataService.applyDraft(storageCode3, draftCode, publishDate4, closeDate4);
        actualRows = searchDataService.getData(new DataCriteria(storageCode4, publishDate4, closeDate4, fields, null, null));
        assertRows(rows4, actualRows);
        // "1" (pT1, cT3), "1" (cT2, cT1), "2" (pT1, pT3), "2" (cT3, cT1), "3" (pT2, pT3) = (pT4, cT4), "3" (cT3, cT2)
        assertEquals(6, searchDataService.getData(new DataCriteria(storageCode4, null, null, fields, null, null)).size());
        FieldSearchCriteria fieldSearchCriteria = new FieldSearchCriteria(id, SearchTypeEnum.EXACT, singletonList(BigInteger.valueOf(1)));
        //test that there are exactly two rows with id=1 (point row was deleted, one was remained unmodified, one was added
        assertEquals(2,
                searchDataService.getData(
                        new DataCriteria(storageCode4, null, null, fields, singletonList(fieldSearchCriteria), null)
                ).size());

    }
}
