DROP TYPE IF EXISTS BL_CL.DIM_SALES_CHANNELS_TYPE CASCADE;
CREATE TYPE BL_CL.DIM_SALES_CHANNELS_TYPE AS (
    SALES_CHANNEL_ID INTEGER,
    SALES_CHANNEL_SRC_ID VARCHAR(100),
    CHANNEL_NAME VARCHAR(100),
    SOURCE_SYSTEM VARCHAR(50),
    SOURCE_ENTITY VARCHAR(50)
);

CREATE OR REPLACE FUNCTION BL_CL.GET_DIM_SALES_CHANNELS_DATA()
RETURNS SETOF BL_CL.DIM_SALES_CHANNELS_TYPE
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        sc.SALES_CHANNEL_ID,
        sc.SALES_CHANNEL_SRC_ID,
        sc.CHANNEL_NAME,
        'BL_3NF'::VARCHAR(50) AS SOURCE_SYSTEM,
        'CE_SALES_CHANNELS'::VARCHAR(50) AS SOURCE_ENTITY
    FROM BL_3NF.CE_SALES_CHANNELS sc
    WHERE sc.SALES_CHANNEL_ID > 0;
END;
$$;

CREATE OR REPLACE PROCEDURE BL_CL.LOAD_DIM_SALES_CHANNELS()
LANGUAGE plpgsql
AS $$
DECLARE
    cur_channels REFCURSOR;
    v_channel_record BL_CL.DIM_SALES_CHANNELS_TYPE;
    v_sql TEXT;
    v_rows_inserted INTEGER := 0;
    v_rows_updated INTEGER := 0;
    v_default_row_count INTEGER := 0;
    v_exists_count INTEGER;
BEGIN
	BEGIN
    INSERT INTO BL_DM.DIM_SALES_CHANNELS (
        SALES_CHANNEL_SURR_ID, SALES_CHANNEL_SRC_ID, CHANNEL_NAME, 
        SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT
    )
    SELECT -1, '-1', 'n. a.', 'MANUAL', 'DEFAULT_ROW', CURRENT_DATE
    WHERE NOT EXISTS (
        SELECT 1 FROM BL_DM.DIM_SALES_CHANNELS WHERE SALES_CHANNEL_SURR_ID = -1
    );

    GET DIAGNOSTICS v_default_row_count = ROW_COUNT;

    v_sql := 'SELECT * FROM BL_CL.GET_DIM_SALES_CHANNELS_DATA()';
    
    OPEN cur_channels FOR EXECUTE v_sql;
    
    LOOP
        FETCH cur_channels INTO v_channel_record;
        EXIT WHEN NOT FOUND;
        
        SELECT COUNT(*) INTO v_exists_count 
        FROM BL_DM.DIM_SALES_CHANNELS 
        WHERE SALES_CHANNEL_SRC_ID = v_channel_record.SALES_CHANNEL_ID::TEXT;
        
        IF v_exists_count = 0 THEN
            INSERT INTO BL_DM.DIM_SALES_CHANNELS (
                SALES_CHANNEL_SURR_ID, 
                SALES_CHANNEL_SRC_ID,    
                CHANNEL_NAME,
                INSERT_DT, 
                SOURCE_SYSTEM, 
                SOURCE_ENTITY
            )
            VALUES (
                NEXTVAL('bl_dm.dim_sales_channels_seq'),
                v_channel_record.SALES_CHANNEL_ID::TEXT,  
                v_channel_record.CHANNEL_NAME,
                CURRENT_DATE,
                v_channel_record.SOURCE_SYSTEM,
                v_channel_record.SOURCE_ENTITY
            );
            v_rows_inserted := v_rows_inserted + 1;
        ELSE
            UPDATE BL_DM.DIM_SALES_CHANNELS 
            SET 
                CHANNEL_NAME = v_channel_record.CHANNEL_NAME,
                SOURCE_SYSTEM = v_channel_record.SOURCE_SYSTEM,
                SOURCE_ENTITY = v_channel_record.SOURCE_ENTITY,
                UPDATE_DT = CURRENT_DATE
            WHERE SALES_CHANNEL_SRC_ID = v_channel_record.SALES_CHANNEL_ID::TEXT
            AND (
                CHANNEL_NAME IS DISTINCT FROM v_channel_record.CHANNEL_NAME OR
                SOURCE_SYSTEM IS DISTINCT FROM v_channel_record.SOURCE_SYSTEM OR
                SOURCE_ENTITY IS DISTINCT FROM v_channel_record.SOURCE_ENTITY
            );
            
            GET DIAGNOSTICS v_rows_updated = ROW_COUNT;
        END IF;
    END LOOP;
    
    CLOSE cur_channels;

    CALL BL_CL.LOG_PROCEDURE(
        'LOAD_DIM_SALES_CHANNELS',
        v_rows_inserted + v_rows_updated + v_default_row_count,
        'Successfully loaded DIM_SALES_CHANNELS. Inserted: ' || v_rows_inserted || ', Updated: ' || v_rows_updated || ', Default rows: ' || v_default_row_count,
        'Success'
    );

EXCEPTION
    WHEN OTHERS THEN
        CALL BL_CL.LOG_PROCEDURE(
            'LOAD_DIM_SALES_CHANNELS',
            -1,
            'Error: ' || SQLERRM,
            'Error'
        );
        RAISE;
	END;
END;
$$;

CALL BL_CL.LOAD_DIM_SALES_CHANNELS();

SELECT * FROM BL_CL.LOG_TABLE 
WHERE procedure_name = 'LOAD_DIM_SALES_CHANNELS' 
ORDER BY execution_time DESC;

SELECT * FROM BL_DM.DIM_SALES_CHANNELS;