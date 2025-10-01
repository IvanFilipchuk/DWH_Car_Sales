CREATE OR REPLACE FUNCTION BL_CL.FN_GET_DATES_TO_LOAD()
RETURNS TABLE (
    date_value DATE,
    day INTEGER,
    month INTEGER,
    year INTEGER,
    quarter INTEGER,
    source_system VARCHAR(50),
    source_entity VARCHAR(50)
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        date_series::DATE as date_value,
        EXTRACT(DAY FROM date_series)::INT as day,
        EXTRACT(MONTH FROM date_series)::INT as month,
        EXTRACT(YEAR FROM date_series)::INT as year,
        EXTRACT(QUARTER FROM date_series)::INT as quarter,
        'MANUAL'::VARCHAR(50) as source_system,
        'CALENDAR_GENERATION'::VARCHAR(50) as source_entity
    FROM generate_series('2010-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL) AS date_series
    WHERE NOT EXISTS (
        SELECT 1 FROM BL_3NF.CE_DATES d WHERE d.DATE = date_series::DATE
    );
END;
$$;
 
 
CREATE OR REPLACE PROCEDURE BL_CL.LOAD_CE_DATES()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_affected INTEGER := 0;
    v_default_row_count INTEGER := 0;
    rec RECORD;
BEGIN
	BEGIN
    INSERT INTO BL_3NF.CE_DATES (DATE_ID, DATE, DAY, MONTH, YEAR, QUARTER, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT)
    SELECT -1, '1900-01-01'::DATE, 1, 1, 1900, 1, 'MANUAL', 'DEFAULT_ROW', CURRENT_DATE  
    WHERE NOT EXISTS (SELECT 1 FROM BL_3NF.CE_DATES WHERE DATE_ID = -1);
 
    GET DIAGNOSTICS v_default_row_count = ROW_COUNT;
 
    FOR rec IN 
        SELECT 
            date_value, day, month, year, quarter, source_system, source_entity
        FROM BL_CL.FN_GET_DATES_TO_LOAD()
    LOOP
        INSERT INTO BL_3NF.CE_DATES (
            DATE_ID, DATE, DAY, MONTH, YEAR, QUARTER, 
            SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT
        )
        VALUES (
            NEXTVAL('bl_3nf.ce_dates_seq'),
            rec.date_value,
            rec.day,
            rec.month,
            rec.year,
            rec.quarter,
            rec.source_system,
            rec.source_entity,
            CURRENT_DATE
        );
        v_rows_affected := v_rows_affected + 1;
    END LOOP;
 
    CALL BL_CL.LOG_PROCEDURE(
        'LOAD_CE_DATES',
        v_rows_affected + v_default_row_count,
        'Successfully loaded dates. New rows: ' || v_rows_affected || ', Default rows: ' || v_default_row_count,
        'Success'
    );
 
EXCEPTION
    WHEN OTHERS THEN
        CALL BL_CL.LOG_PROCEDURE(
            'LOAD_CE_DATES',
            -1,
            'Error: ' || SQLERRM,
            'Error'
        );
        RAISE;
	END;
END;
$$;
 
CALL BL_CL.LOAD_CE_DATES();
 
SELECT * FROM BL_CL.LOG_TABLE 
WHERE procedure_name = 'LOAD_CE_DATES' 
ORDER BY execution_time DESC;
 
SELECT COUNT(*) FROM BL_3NF.CE_DATES;
SELECT * FROM BL_3NF.CE_DATES;