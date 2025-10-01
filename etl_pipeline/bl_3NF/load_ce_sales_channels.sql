CREATE OR REPLACE FUNCTION BL_CL.FN_GET_SALES_CHANNELS_TO_LOAD()
RETURNS TABLE (
    sales_channel_src_id VARCHAR(100),
    channel_name VARCHAR(100),
    source_system VARCHAR(50),
    source_entity VARCHAR(50)
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT
        UPPER(TRIM(saleschannel))::VARCHAR(100) AS sales_channel_src_id,
        UPPER(TRIM(saleschannel))::VARCHAR(100) AS channel_name,
        'SA_CAR_SALES_OFFLINE'::VARCHAR(50) AS source_system,
        'SRC_CAR_SALES_OFFLINE'::VARCHAR(50) AS source_entity
    FROM sa_car_sales_offline.src_car_sales_offline
    WHERE TRIM(saleschannel) IS NOT NULL
        AND TRIM(saleschannel) != ''
 
    UNION ALL
 
    SELECT DISTINCT
        UPPER(TRIM(sales_channel))::VARCHAR(100) AS sales_channel_src_id,
        UPPER(TRIM(sales_channel))::VARCHAR(100) AS channel_name,
        'SA_CAR_SALES_ONLINE'::VARCHAR(50) AS source_system,
        'SRC_CAR_SALES_ONLINE'::VARCHAR(50) AS source_entity
    FROM sa_car_sales_online.src_car_sales_online
    WHERE TRIM(sales_channel) IS NOT NULL
        AND TRIM(sales_channel) != ''
        AND NOT EXISTS (
            SELECT 1 FROM sa_car_sales_offline.src_car_sales_offline 
            WHERE UPPER(TRIM(saleschannel)) = UPPER(TRIM(sales_channel))
        );
END;
$$;


CREATE OR REPLACE PROCEDURE BL_CL.LOAD_CE_SALES_CHANNELS()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_affected INTEGER := 0;
    v_default_row_count INTEGER := 0;
    rec RECORD;
BEGIN
	BEGIN
    INSERT INTO BL_3NF.CE_SALES_CHANNELS (SALES_CHANNEL_ID, SALES_CHANNEL_SRC_ID, CHANNEL_NAME, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT)
    SELECT -1, 'n. a.', 'n. a.', 'MANUAL', 'DEFAULT_ROW', CURRENT_DATE  
    WHERE NOT EXISTS (SELECT 1 FROM BL_3NF.CE_SALES_CHANNELS WHERE SALES_CHANNEL_ID = -1);
 
    GET DIAGNOSTICS v_default_row_count = ROW_COUNT;
 
    FOR rec IN 
        SELECT DISTINCT 
            sales_channel_src_id, channel_name, source_system, source_entity
        FROM BL_CL.FN_GET_SALES_CHANNELS_TO_LOAD() f
        WHERE NOT EXISTS (
            SELECT 1 FROM BL_3NF.CE_SALES_CHANNELS sc 
            WHERE sc.SALES_CHANNEL_SRC_ID = f.sales_channel_src_id
        )
    LOOP
        INSERT INTO BL_3NF.CE_SALES_CHANNELS (
            SALES_CHANNEL_ID, SALES_CHANNEL_SRC_ID, CHANNEL_NAME, 
            SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT
        )
        VALUES (
            NEXTVAL('bl_3nf.ce_sales_channels_seq'),
            rec.sales_channel_src_id,
            rec.channel_name,
            rec.source_system,
            rec.source_entity,
            CURRENT_DATE
        );
        v_rows_affected := v_rows_affected + 1;
    END LOOP;
 
    CALL BL_CL.LOG_PROCEDURE(
        'LOAD_CE_SALES_CHANNELS',
        v_rows_affected + v_default_row_count,
        'Successfully loaded sales channels. New rows: ' || v_rows_affected || ', Default rows: ' || v_default_row_count,
        'Success'
    );
 
EXCEPTION
    WHEN OTHERS THEN
        CALL BL_CL.LOG_PROCEDURE(
            'LOAD_CE_SALES_CHANNELS',
            -1,
            'Error: ' || SQLERRM,
            'Error'
        );
        RAISE;
	END;
END;
$$;

CALL BL_CL.LOAD_CE_SALES_CHANNELS();
 
SELECT * FROM BL_CL.LOG_TABLE 
WHERE procedure_name = 'LOAD_CE_SALES_CHANNELS' 
ORDER BY execution_time DESC;
 
SELECT COUNT(*) FROM BL_3NF.CE_SALES_CHANNELS;
SELECT * FROM BL_3NF.CE_SALES_CHANNELS;