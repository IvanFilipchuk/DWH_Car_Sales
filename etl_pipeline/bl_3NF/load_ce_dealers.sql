CREATE OR REPLACE FUNCTION BL_CL.FN_GET_DEALERS_TO_LOAD()
RETURNS TABLE (
    dealer_src_id VARCHAR(100),
    dealer_name VARCHAR(100),
    source_system VARCHAR(50),
    source_entity VARCHAR(50)
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT
        UPPER(TRIM(cardelername))::VARCHAR(100) AS dealer_src_id,
        UPPER(TRIM(cardelername))::VARCHAR(100) AS dealer_name,
        'SA_CAR_SALES_OFFLINE'::VARCHAR(50) AS source_system,
        'SRC_CAR_SALES_OFFLINE'::VARCHAR(50) AS source_entity
    FROM sa_car_sales_offline.src_car_sales_offline
    WHERE TRIM(cardelername) IS NOT NULL
        AND TRIM(cardelername) != ''
 
    UNION ALL
 
    SELECT DISTINCT
        UPPER(TRIM(car_dealer_name))::VARCHAR(100) AS dealer_src_id,
        UPPER(TRIM(car_dealer_name))::VARCHAR(100) AS dealer_name,
        'SA_CAR_SALES_ONLINE'::VARCHAR(50) AS source_system,
        'SRC_CAR_SALES_ONLINE'::VARCHAR(50) AS source_entity
    FROM sa_car_sales_online.src_car_sales_online
    WHERE TRIM(car_dealer_name) IS NOT NULL
        AND TRIM(car_dealer_name) != ''
        AND NOT EXISTS (
            SELECT 1 FROM sa_car_sales_offline.src_car_sales_offline 
            WHERE UPPER(TRIM(cardelername)) = UPPER(TRIM(car_dealer_name))
        );
END;
$$;
 
CREATE OR REPLACE PROCEDURE BL_CL.LOAD_CE_DEALERS()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_affected INTEGER := 0;
    v_default_row_count INTEGER := 0;
    rec RECORD;
BEGIN
	BEGIN
    INSERT INTO BL_3NF.CE_DEALERS (DEALER_ID, DEALER_SRC_ID, DEALER_NAME, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT)
    SELECT -1, 'n. a.', 'n. a.', 'MANUAL', 'DEFAULT_ROW', CURRENT_DATE  
    WHERE NOT EXISTS (SELECT 1 FROM BL_3NF.CE_DEALERS WHERE DEALER_ID = -1);
 
    GET DIAGNOSTICS v_default_row_count = ROW_COUNT;
 
    FOR rec IN 
        SELECT DISTINCT 
            dealer_src_id, dealer_name, source_system, source_entity
        FROM BL_CL.FN_GET_DEALERS_TO_LOAD() f
        WHERE NOT EXISTS (
            SELECT 1 FROM BL_3NF.CE_DEALERS d 
            WHERE d.DEALER_SRC_ID = f.dealer_src_id
        )
    LOOP
        INSERT INTO BL_3NF.CE_DEALERS (
            DEALER_ID, DEALER_SRC_ID, DEALER_NAME, 
            SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT
        )
        VALUES (
            NEXTVAL('bl_3nf.ce_dealers_seq'),
            rec.dealer_src_id,
            rec.dealer_name,
            rec.source_system,
            rec.source_entity,
            CURRENT_DATE
        );
        v_rows_affected := v_rows_affected + 1;
    END LOOP;
 
    CALL BL_CL.LOG_PROCEDURE(
        'LOAD_CE_DEALERS',
        v_rows_affected + v_default_row_count,
        'Successfully loaded dealers. New rows: ' || v_rows_affected || ', Default rows: ' || v_default_row_count,
        'Success'
    );
 
EXCEPTION
    WHEN OTHERS THEN
        CALL BL_CL.LOG_PROCEDURE(
            'LOAD_CE_DEALERS',
            -1,
            'Error: ' || SQLERRM,
            'Error'
        );
        RAISE;
	END;
END;
$$;
 
CALL BL_CL.LOAD_CE_DEALERS();
 
SELECT * FROM BL_CL.LOG_TABLE 
WHERE procedure_name = 'LOAD_CE_DEALERS' 
ORDER BY execution_time DESC;
 
SELECT COUNT(*) FROM BL_3NF.CE_DEALERS;
SELECT * FROM BL_3NF.CE_DEALERS;