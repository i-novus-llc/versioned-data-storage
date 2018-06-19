package ru.i_novus.platform.versioned_data_storage;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
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
import ru.i_novus.platform.datastorage.temporal.model.value.ReferenceFieldValue;
import ru.i_novus.platform.datastorage.temporal.model.value.RowValue;
import ru.i_novus.platform.datastorage.temporal.service.DraftDataService;
import ru.i_novus.platform.datastorage.temporal.service.FieldFactory;
import ru.i_novus.platform.datastorage.temporal.service.SearchDataService;
import ru.i_novus.platform.versioned_data_storage.config.VersionedDataStorageConfig;
import ru.i_novus.platform.versioned_data_storage.pg_impl.service.FieldFactoryImpl;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by tnurdinov on 08.06.2018.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {JpaTestConfig.class, VersionedDataStorageConfig.class})
public class UseCaseTest {

    private static final Logger logger = LoggerFactory.getLogger(UseCaseTest.class);

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
                d_a_id.valueOf(1),
                d_a_name.valueOf("test"),
                d_a_dateCol.valueOf(LocalDate.now()),
                d_a_boolCol.valueOf(true),
                d_a_floatCol.valueOf(5f)));
        d_a_rows.add(new LongRowValue(
                d_a_id.valueOf(2),
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
        Assert.assertEquals(0, s_a_actualRows.size());
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
                d_b_id.valueOf(1),
                d_b_name.valueOf("name"),
                d_b_ref.valueOf(new Reference(s_a_storageCode, s_a_publishTime, d_a_id.getName(), d_a_name.getName(), "1", "test"))
        );
        draftDataService.addRows(d_b_draftCode, Arrays.asList(d_b_rowValue));
        List<RowValue> d_b_actualRows = searchDataService.getData(new DataCriteria(d_b_draftCode, null, null, d_b_fields, null, "name"));
        Object systemId = d_b_actualRows.get(0).getSystemId();

        d_b_rowValue = new LongRowValue(
                d_b_id.valueOf(1),
                d_b_name.valueOf("name"),
                d_b_ref.valueOf(new Reference(s_a_storageCode, s_a_publishTime, d_a_id.getName(), d_a_name.getName(), "2"))
        );
        d_b_rowValue.setSystemId(systemId);
        draftDataService.updateRow(d_b_draftCode, d_b_rowValue);
        d_b_actualRows = searchDataService.getData(new DataCriteria(d_b_draftCode, null, null, d_b_fields, null, "name"));
        d_b_rowValue.getFieldValues().forEach(value -> {
            if(value instanceof ReferenceFieldValue) {
                ((ReferenceFieldValue) value).getValue().setDisplayValue("test2");
            }
        });
        assertRows(Arrays.asList(d_b_rowValue), d_b_actualRows);
        logger.info("<<<<<<<<<<<<<<< 2 этап завершен >>>>>>>>>>>>>>>>>>>>>");


        logger.info("<<<<<<<<<<<<<<< 4 этап >>>>>>>>>>>>>>>>>>>>>");
        Date s_b_publishTime = new Date();
        String s_b_storageCode = draftDataService.applyDraft(null, d_b_draftCode, s_b_publishTime);
        Collection<RowValue> s_b_actualRows = searchDataService.getData(new DataCriteria(s_b_storageCode, null, null, d_b_fields, null, null));
        assertRows(Arrays.asList(d_b_rowValue), s_b_actualRows);
        logger.info("<<<<<<<<<<<<<<< 4 этап завершен >>>>>>>>>>>>>>>>>>>>>");

    }

    private void assertRows(List<RowValue> expectedRows, Collection<RowValue> actualRows) {
        Assert.assertEquals("result size not equals", expectedRows.size(), actualRows.size());
        Assert.assertTrue(
                "not equals actualRows: \n" + actualRows.stream().map(RowValue::toString).collect(Collectors.joining(", ")) + " \n and expected rows: \n" + expectedRows.stream().map(RowValue::toString).collect(Collectors.joining(", "))
                , actualRows.stream().filter(actualRow -> expectedRows.stream().filter(expectedRow -> equalsFieldValues(expectedRow.getFieldValues(), actualRow.getFieldValues())).findAny().isPresent()).findAny().isPresent());
    }

    private boolean equalsFieldValues(List<FieldValue> values1, List<FieldValue> values2) {
        if (values1 == values2) return true;
        if (values1 == null || values2 == null || values1.size() != values2.size()) return false;

        for (FieldValue val1 : values1) {
            boolean isPresent = values2.stream().filter(val2 -> {
                if (val2 == val1) return true;
                if (val2.getField().equals(val1.getField()))
                    if (val1 instanceof ReferenceFieldValue) {
                        return ((ReferenceFieldValue) val2).getValue().getValue().equals(((ReferenceFieldValue) val1).getValue().getValue())
                                && ((ReferenceFieldValue) val2).getValue().getDisplayValue().equals(((ReferenceFieldValue) val1).getValue().getDisplayValue());
                    } else {
                        return val2.equals(val1);
                    }
                return false;

            }).findAny().isPresent();
            if(!isPresent) {
                return false;
            }

        }

        return true;

    }



}
