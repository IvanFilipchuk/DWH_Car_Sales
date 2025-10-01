DROP TYPE IF EXISTS BL_CL.DIM_DATES_TYPE CASCADE;
CREATE TYPE BL_CL.DIM_DATES_TYPE AS (
    DATE_ID INTEGER,
    DATE_VALUE DATE,
    DAY INTEGER,
    MONTH INTEGER,
    YEAR INTEGER,
    QUARTER INTEGER,
    IS_WEEKEND CHAR(1),
    MONTH_NAME VARCHAR(20),
    DAY_OF_WEEK VARCHAR(20),
    SOURCE_SYSTEM VARCHAR(50),
    SOURCE_ENTITY VARCHAR(50)
);

CREATE OR REPLACE FUNCTION BL_CL.GET_DIM_DATES_DATA()
RETURNS SETOF BL_CL.DIM_DATES_TYPE
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        d.DATE_ID,
        d.DATE,
        d.DAY,
        d.MONTH,
        d.YEAR,
        d.QUARTER,
        CASE WHEN EXTRACT(DOW FROM d.DATE) IN (0, 6) THEN 'Y' ELSE 'N' END::CHAR(1) as IS_WEEKEND,
        TO_CHAR(d.DATE, 'Month')::VARCHAR(20) as MONTH_NAME,
        TO_CHAR(d.DATE, 'Day')::VARCHAR(20) as DAY_OF_WEEK,
        'BL_3NF'::VARCHAR(50) AS SOURCE_SYSTEM,
        'CE_DATES'::VARCHAR(50) AS SOURCE_ENTITY
    FROM BL_3NF.CE_DATES d
    WHERE d.DATE_ID > 0;
END;
$$;

CREATE OR REPLACE PROCEDURE BL_CL.LOAD_DIM_DATES()
LANGUAGE plpgsql
AS $$
DECLARE
    cur_dates REFCURSOR;
    v_date_record BL_CL.DIM_DATES_TYPE;
    v_sql TEXT;
    v_rows_inserted INTEGER := 0;
    v_rows_updated INTEGER := 0;
    v_default_row_count INTEGER := 0;
    v_exists_count INTEGER;
BEGIN
	BEGIN
    INSERT INTO BL_DM.DIM_DATES (
        DATE_SURR_ID, DATE, DAY, MONTH, YEAR, QUARTER, IS_WEEKEND, 
        MONTH_NAME, DAY_OF_WEEK, INSERT_DT, SOURCE_SYSTEM, SOURCE_ENTITY
    )
    SELECT -1, '1900-01-01'::DATE, -1, -1, -1, -1,'N', 'n. a.', 'n. a.', CURRENT_DATE, 'MANUAL', 'DEFAULT_ROW'
    WHERE NOT EXISTS (
        SELECT 1 FROM BL_DM.DIM_DATES WHERE DATE_SURR_ID = -1
    );

    GET DIAGNOSTICS v_default_row_count = ROW_COUNT;

    v_sql := 'SELECT * FROM BL_CL.GET_DIM_DATES_DATA()';
    
    OPEN cur_dates FOR EXECUTE v_sql;
    
    LOOP
        FETCH cur_dates INTO v_date_record;
        EXIT WHEN NOT FOUND;
        
        SELECT COUNT(*) INTO v_exists_count 
        FROM BL_DM.DIM_DATES 
        WHERE DATE = v_date_record.DATE_VALUE;
        
        IF v_exists_count = 0 THEN
            INSERT INTO BL_DM.DIM_DATES (
                DATE_SURR_ID, 
                DATE,
                DAY,
                MONTH,
                YEAR,
                QUARTER,
                IS_WEEKEND,
                MONTH_NAME,
                DAY_OF_WEEK,
                INSERT_DT, 
                SOURCE_SYSTEM, 
                SOURCE_ENTITY
            )
            VALUES (
                NEXTVAL('bl_dm.dim_dates_seq'),
                v_date_record.DATE_VALUE,
                v_date_record.DAY,
                v_date_record.MONTH,
                v_date_record.YEAR,
                v_date_record.QUARTER,
                v_date_record.IS_WEEKEND,
                v_date_record.MONTH_NAME,
                v_date_record.DAY_OF_WEEK,
                CURRENT_DATE,
                v_date_record.SOURCE_SYSTEM,
                v_date_record.SOURCE_ENTITY
            );
            v_rows_inserted := v_rows_inserted + 1;
        ELSE
            UPDATE BL_DM.DIM_DATES 
            SET 
                DAY = v_date_record.DAY,
                MONTH = v_date_record.MONTH,
                YEAR = v_date_record.YEAR,
                QUARTER = v_date_record.QUARTER,
                IS_WEEKEND = v_date_record.IS_WEEKEND,
                MONTH_NAME = v_date_record.MONTH_NAME,
                DAY_OF_WEEK = v_date_record.DAY_OF_WEEK,
                SOURCE_SYSTEM = v_date_record.SOURCE_SYSTEM,
                SOURCE_ENTITY = v_date_record.SOURCE_ENTITY,
                UPDATE_DT = CURRENT_DATE
            WHERE DATE = v_date_record.DATE_VALUE
            AND (
                DAY IS DISTINCT FROM v_date_record.DAY OR
                MONTH IS DISTINCT FROM v_date_record.MONTH OR
                YEAR IS DISTINCT FROM v_date_record.YEAR OR
                QUARTER IS DISTINCT FROM v_date_record.QUARTER OR
                IS_WEEKEND IS DISTINCT FROM v_date_record.IS_WEEKEND OR
                MONTH_NAME IS DISTINCT FROM v_date_record.MONTH_NAME OR
                DAY_OF_WEEK IS DISTINCT FROM v_date_record.DAY_OF_WEEK OR
                SOURCE_SYSTEM IS DISTINCT FROM v_date_record.SOURCE_SYSTEM OR
                SOURCE_ENTITY IS DISTINCT FROM v_date_record.SOURCE_ENTITY
            );
            
            GET DIAGNOSTICS v_rows_updated = ROW_COUNT;
        END IF;
    END LOOP;
    
    CLOSE cur_dates;

    CALL BL_CL.LOG_PROCEDURE(
        'LOAD_DIM_DATES',
        v_rows_inserted + v_rows_updated + v_default_row_count,
        'Successfully loaded DIM_DATES. Inserted: ' || v_rows_inserted || ', Updated: ' || v_rows_updated || ', Default rows: ' || v_default_row_count,
        'Success'
    );

EXCEPTION
    WHEN OTHERS THEN
        CALL BL_CL.LOG_PROCEDURE(
            'LOAD_DIM_DATES',
            -1,
            'Error: ' || SQLERRM,
            'Error'
        );
        RAISE;
	END;
END;
$$;

CALL BL_CL.LOAD_DIM_DATES();

SELECT * FROM BL_CL.LOG_TABLE 
WHERE procedure_name = 'LOAD_DIM_DATES' 
ORDER BY execution_time DESC;

SELECT * FROM BL_DM.DIM_DATES;