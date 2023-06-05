 CREATE SCHEMA IF NOT EXISTS data;

DROP FUNCTION IF EXISTS data.closed_now_records(fields text, id BIGINT, tbl text, from_dt TIMESTAMP WITH TIME ZONE,
                                                                           to_dt  TIMESTAMP WITH TIME ZONE, tbl_seq_name text);

CREATE OR REPLACE FUNCTION data.closed_now_records(fields text, id BIGINT, tbl text, from_dt TIMESTAMP WITHOUT TIME ZONE,
                                                                           to_dt  TIMESTAMP WITHOUT TIME ZONE, tbl_seq_name text)
                          RETURNS SETOF RECORD AS
                        $BODY$
DECLARE _left  RECORD;
        _right RECORD;
        r     RECORD;
BEGIN
  IF (from_dt IS NULL)
  THEN
    from_dt := -infinity :: TIMESTAMP WITHOUT TIME ZONE ;
  END IF;

  IF (to_dt IS NULL)
  THEN
    to_dt := infinity :: TIMESTAMP WITHOUT TIME ZONE ;
  END IF;

  EXECUTE format(
      'select %1$s from %2$s where "SYS_RECORDID" = %3$s',
      fields, tbl, id)
  INTO r;
  -- строка содержится в интервале времени
  IF (from_dt <= coalesce(r."SYS_PUBLISHTIME", '-infinity') AND coalesce(r."SYS_CLOSETIME", 'infinity') <= to_dt)
  THEN
    RETURN;
  ELSE
    --отрезаем левый конец
    IF (coalesce(r."SYS_PUBLISHTIME", '-infinity') < from_dt AND from_dt < coalesce(r."SYS_CLOSETIME", 'infinity'))
    THEN
      _left := r;
      _left."SYS_RECORDID" := nextval(tbl_seq_name);
      _left."SYS_CLOSETIME" := from_dt;
      RETURN NEXT _left;
    END IF;
    --отрезаем правый конец
    IF (coalesce(r."SYS_PUBLISHTIME", '-infinity') < to_dt AND to_dt < coalesce(r."SYS_CLOSETIME", 'infinity'))
    THEN
      _right := r;
      _right."SYS_RECORDID" := nextval(tbl_seq_name);
      _right."SYS_PUBLISHTIME" := to_dt;
      RETURN NEXT _right;
    END IF;
  END IF;

END;
$BODY$
LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS data.merged_actual_rows(fields TEXT, sys_hash CHAR(32), tableName text,
                                                     from_dt TIMESTAMP WITH TIME ZONE,
                                                     to_dt TIMESTAMP WITH TIME ZONE, sys_rec_id bigint);

/**
* sys_hash - хэш актуальной записи
* tableName - таблица версий
* from_dt, to_dt - даты при публикации
* fields - поля справочника
* sys_rec_id - предзаполненый идентификатор записи
*/
CREATE OR REPLACE FUNCTION data.merged_actual_rows(fields TEXT, sys_hash CHAR(32), tableName text,
                                                     from_dt TIMESTAMP WITHOUT TIME ZONE,
                                                     to_dt TIMESTAMP WITHOUT TIME ZONE, sys_rec_id bigint)
  RETURNS SETOF RECORD AS
$BODY$
DECLARE r   RECORD;
	result RECORD;
        pTime TIMESTAMP WITHOUT TIME ZONE;
        cTime TIMESTAMP WITHOUT TIME ZONE;
        has_result boolean;


BEGIN
	has_result :=false;
	IF(from_dt IS NULL) THEN
	   from_dt := -infinity;
	END IF;

	IF(to_dt IS NULL) THEN
	   to_dt := infinity;
	END IF;

	pTime := from_dt;
	cTime := to_dt;

	FOR r IN EXECUTE format ('SELECT "SYS_RECORDID", %1$s, "FTS", "SYS_HASH", "SYS_PUBLISHTIME", "SYS_CLOSETIME"  FROM %2$s
	where "SYS_HASH" = ''%3$s''
	AND  (( ("SYS_PUBLISHTIME", coalesce("SYS_CLOSETIME", ''infinity'' )) OVERLAPS (timestamp without time zone ''%4$s'', timestamp without time zone ''%5$s'') )
	OR coalesce("SYS_CLOSETIME", ''infinity'') = timestamp without time zone ''%4$s'')
	', fields, tableName, sys_hash, from_dt, to_dt)
	 LOOP
		pTime := LEAST(pTime, coalesce(r."SYS_PUBLISHTIME", '-infinity'));
		cTime := GREATEST(cTime, coalesce(r."SYS_CLOSETIME", 'infinity'));
		result := r;
		result."SYS_PUBLISHTIME" := pTime;
		result."SYS_CLOSETIME" := cTime;
		result."SYS_RECORDID" := sys_rec_id;
		has_result := true;
	END LOOP;

  IF(has_result) THEN
    RAISE INFO 'result exists';
    RETURN NEXT result;
  ELSE
	RETURN;
    END IF;
END;
$BODY$
LANGUAGE plpgsql;

