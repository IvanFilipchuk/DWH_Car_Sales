DROP TYPE IF EXISTS BL_CL.DIM_DEALERS_TYPE CASCADE;
CREATE TYPE BL_CL.DIM_DEALERS_TYPE AS (
    DEALER_ID INTEGER,
    DEALER_SRC_ID VARCHAR(100),
    DEALER_NAME VARCHAR(100),       
    SOURCE_SYSTEM VARCHAR(50),
    SOURCE_ENTITY VARCHAR(50)
);

CREATE OR REPLACE FUNCTION BL_CL.GET_DIM_DEALERS_DATA()
RETURNS SETOF BL_CL.DIM_DEALERS_TYPE
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        d.DEALER_ID,
        d.DEALER_SRC_ID,
        d.DEALER_NAME,
        'BL_3NF'::VARCHAR(50) AS SOURCE_SYSTEM,
        'CE_DEALERS'::VARCHAR(50) AS SOURCE_ENTITY
    FROM BL_3NF.CE_DEALERS d
    WHERE d.DEALER_ID > 0; 
END;
$$;

CREATE OR REPLACE PROCEDURE BL_CL.LOAD_DIM_DEALERS()
LANGUAGE plpgsql
AS $$
DECLARE
    cur_dealers REFCURSOR;
    v_dealer_record BL_CL.DIM_DEALERS_TYPE;
    v_sql TEXT;
    v_rows_inserted INTEGER := 0;
    v_rows_updated INTEGER := 0;
    v_default_row_count INTEGER := 0;
    v_exists_count INTEGER;
BEGIN
	BEGIN
    INSERT INTO BL_DM.DIM_DEALERS (
        DEALER_SURR_ID, DEALER_SRC_ID, DEALER_NAME,
        INSERT_DT, SOURCE_SYSTEM, SOURCE_ENTITY
    )
    SELECT -1, '-1', 'n. a.',
           CURRENT_DATE, 'MANUAL', 'DEFAULT_ROW'
    WHERE NOT EXISTS (
        SELECT 1 FROM BL_DM.DIM_DEALERS WHERE DEALER_SURR_ID = -1
    );

    GET DIAGNOSTICS v_default_row_count = ROW_COUNT;

    v_sql := 'SELECT * FROM BL_CL.GET_DIM_DEALERS_DATA()';
    
    OPEN cur_dealers FOR EXECUTE v_sql;
    
    LOOP
        FETCH cur_dealers INTO v_dealer_record;
        EXIT WHEN NOT FOUND;
        
        SELECT COUNT(*) INTO v_exists_count 
        FROM BL_DM.DIM_DEALERS 
        WHERE DEALER_SRC_ID = v_dealer_record.DEALER_ID::TEXT;
        
        IF v_exists_count = 0 THEN
            INSERT INTO BL_DM.DIM_DEALERS (
                DEALER_SURR_ID, 
                DEALER_SRC_ID,     
                DEALER_NAME,
                INSERT_DT, 
                SOURCE_SYSTEM, 
                SOURCE_ENTITY
            )
            VALUES (
                NEXTVAL('bl_dm.dim_dealers_seq'),
                v_dealer_record.DEALER_ID::TEXT,
                v_dealer_record.DEALER_NAME,
                CURRENT_DATE,
                v_dealer_record.SOURCE_SYSTEM,
                v_dealer_record.SOURCE_ENTITY
            );
            v_rows_inserted := v_rows_inserted + 1;
        ELSE
            UPDATE BL_DM.DIM_DEALERS 
            SET 
                DEALER_NAME = v_dealer_record.DEALER_NAME,
                SOURCE_SYSTEM = v_dealer_record.SOURCE_SYSTEM,
                SOURCE_ENTITY = v_dealer_record.SOURCE_ENTITY,
                UPDATE_DT = CURRENT_DATE
            WHERE DEALER_SRC_ID = v_dealer_record.DEALER_ID::TEXT
            AND (
                DEALER_NAME IS DISTINCT FROM v_dealer_record.DEALER_NAME OR
                SOURCE_SYSTEM IS DISTINCT FROM v_dealer_record.SOURCE_SYSTEM OR
                SOURCE_ENTITY IS DISTINCT FROM v_dealer_record.SOURCE_ENTITY
            );
            
            GET DIAGNOSTICS v_rows_updated = ROW_COUNT;
        END IF;
    END LOOP;
    
    CLOSE cur_dealers;

    CALL BL_CL.LOG_PROCEDURE(
        'LOAD_DIM_DEALERS',
        v_rows_inserted + v_rows_updated + v_default_row_count,
        'Successfully loaded DIM_DEALERS. Inserted: ' || v_rows_inserted || ', Updated: ' || v_rows_updated || ', Default rows: ' || v_default_row_count,
        'Success'
    );

EXCEPTION
    WHEN OTHERS THEN
        CALL BL_CL.LOG_PROCEDURE(
            'LOAD_DIM_DEALERS',
            -1,
            'Error: ' || SQLERRM,
            'Error'
        );
        RAISE;
	END;
END;
$$;

CALL BL_CL.LOAD_DIM_DEALERS();

SELECT * FROM BL_CL.LOG_TABLE 
WHERE procedure_name = 'LOAD_DIM_DEALERS' 
ORDER BY execution_time DESC;

SELECT * FROM BL_DM.DIM_DEALERS;