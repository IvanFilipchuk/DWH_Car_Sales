CREATE OR REPLACE FUNCTION BL_CL.FN_GET_CAR_BRANDS_TO_LOAD()
RETURNS TABLE (
    brand_src_id VARCHAR(100),
    brand_name VARCHAR(100),
    source_system VARCHAR(50),
    source_entity VARCHAR(50)
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT
        UPPER(TRIM(carmake))::VARCHAR(100) AS brand_src_id,
        UPPER(TRIM(carmake))::VARCHAR(100) AS brand_name,
        'SA_CAR_SALES_OFFLINE'::VARCHAR(50) AS source_system,
        'SRC_CAR_SALES_OFFLINE'::VARCHAR(50) AS source_entity
    FROM sa_car_sales_offline.src_car_sales_offline
    WHERE TRIM(carmake) IS NOT NULL
        AND TRIM(carmake) != ''
 
    UNION ALL
 
    SELECT DISTINCT
        UPPER(TRIM(brand))::VARCHAR(100) AS brand_src_id,
        UPPER(TRIM(brand))::VARCHAR(100) AS brand_name,
        'SA_CAR_SALES_ONLINE'::VARCHAR(50) AS source_system,
        'SRC_CAR_SALES_ONLINE'::VARCHAR(50) AS source_entity
    FROM sa_car_sales_online.src_car_sales_online
    WHERE TRIM(brand) IS NOT NULL
        AND TRIM(brand) != ''
        AND NOT EXISTS (
            SELECT 1 FROM sa_car_sales_offline.src_car_sales_offline 
            WHERE UPPER(TRIM(carmake)) = UPPER(TRIM(brand))
        );
END;
$$;
 
CREATE OR REPLACE PROCEDURE BL_CL.LOAD_CE_CAR_BRANDS()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_affected INTEGER := 0;
    v_default_row_count INTEGER := 0;
    rec RECORD;
BEGIN
	BEGIN
    INSERT INTO BL_3NF.CE_CAR_BRANDS (CAR_BRAND_ID, BRAND_SRC_ID, BRAND_NAME, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT)
    SELECT -1, 'n. a.', 'n. a.', 'MANUAL', 'DEFAULT_ROW', CURRENT_DATE  
    WHERE NOT EXISTS (SELECT 1 FROM BL_3NF.CE_CAR_BRANDS WHERE CAR_BRAND_ID = -1);
 
    GET DIAGNOSTICS v_default_row_count = ROW_COUNT;
 
    FOR rec IN 
        SELECT DISTINCT 
            brand_src_id, brand_name, source_system, source_entity
        FROM BL_CL.FN_GET_CAR_BRANDS_TO_LOAD() f
        WHERE NOT EXISTS (
            SELECT 1 FROM BL_3NF.CE_CAR_BRANDS cb 
            WHERE cb.BRAND_SRC_ID = f.brand_src_id
        )
    LOOP
        INSERT INTO BL_3NF.CE_CAR_BRANDS (
            CAR_BRAND_ID, BRAND_SRC_ID, BRAND_NAME, 
            SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT
        )
        VALUES (
            NEXTVAL('bl_3nf.ce_car_brands_seq'),
            rec.brand_src_id,
            rec.brand_name,
            rec.source_system,
            rec.source_entity,
            CURRENT_DATE
        );
        v_rows_affected := v_rows_affected + 1;
    END LOOP;
 
    CALL BL_CL.LOG_PROCEDURE(
        'LOAD_CE_CAR_BRANDS',
        v_rows_affected + v_default_row_count,
        'Successfully loaded car brands. New rows: ' || v_rows_affected || ', Default rows: ' || v_default_row_count,
        'Success'
    );
 
EXCEPTION
    WHEN OTHERS THEN
        CALL BL_CL.LOG_PROCEDURE(
            'LOAD_CE_CAR_BRANDS',
            -1,
            'Error: ' || SQLERRM,
            'Error'
        );
        RAISE;
	END;
END;
$$;
 
CALL BL_CL.LOAD_CE_CAR_BRANDS();
 
SELECT * FROM BL_CL.LOG_TABLE 
WHERE procedure_name = 'LOAD_CE_CAR_BRANDS' 
ORDER BY execution_time DESC;
 
SELECT * FROM BL_3NF.CE_CAR_BRANDS ;