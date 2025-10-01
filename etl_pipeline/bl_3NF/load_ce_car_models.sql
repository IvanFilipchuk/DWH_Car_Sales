CREATE OR REPLACE FUNCTION BL_CL.FN_GET_CAR_MODELS_TO_LOAD()
RETURNS TABLE (
    model_src_id VARCHAR(200),
    car_brand_id INTEGER,
    model_name VARCHAR(100),
    source_system VARCHAR(50),
    source_entity VARCHAR(50)
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT
        CONCAT(UPPER(TRIM(o.carmodel)), '_', COALESCE(b.CAR_BRAND_ID, -1))::VARCHAR(200) AS model_src_id,
        COALESCE(b.CAR_BRAND_ID, -1) AS car_brand_id,
        UPPER(TRIM(o.carmodel))::VARCHAR(100) AS model_name,
        'SA_CAR_SALES_OFFLINE'::VARCHAR(50) AS source_system,
        'SRC_CAR_SALES_OFFLINE'::VARCHAR(50) AS source_entity
    FROM sa_car_sales_offline.src_car_sales_offline o
    LEFT JOIN BL_3NF.CE_CAR_BRANDS b ON b.BRAND_SRC_ID = UPPER(TRIM(o.carmake))
        AND b.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    WHERE TRIM(o.carmodel) IS NOT NULL
        AND TRIM(o.carmodel) != ''
        AND TRIM(o.carmake) IS NOT NULL
        AND TRIM(o.carmake) != ''
 
    UNION ALL
 
    SELECT DISTINCT
        CONCAT(UPPER(TRIM(o.car_model)), '_', COALESCE(b.CAR_BRAND_ID, -1))::VARCHAR(200) AS model_src_id,
        COALESCE(b.CAR_BRAND_ID, -1) AS car_brand_id,
        UPPER(TRIM(o.car_model))::VARCHAR(100) AS model_name,
        'SA_CAR_SALES_ONLINE'::VARCHAR(50) AS source_system,
        'SRC_CAR_SALES_ONLINE'::VARCHAR(50) AS source_entity
    FROM sa_car_sales_online.src_car_sales_online o
    LEFT JOIN BL_3NF.CE_CAR_BRANDS b ON b.BRAND_SRC_ID = UPPER(TRIM(o.brand))
        AND b.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    WHERE TRIM(o.car_model) IS NOT NULL
        AND TRIM(o.car_model) != ''
        AND TRIM(o.brand) IS NOT NULL
        AND TRIM(o.brand) != ''
        AND NOT EXISTS (
            SELECT 1 FROM sa_car_sales_offline.src_car_sales_offline o2
            LEFT JOIN BL_3NF.CE_CAR_BRANDS b2 ON b2.BRAND_SRC_ID = UPPER(TRIM(o2.carmake))
            WHERE CONCAT(UPPER(TRIM(o2.carmodel)), '_', COALESCE(b2.CAR_BRAND_ID, -1))  
                  = CONCAT(UPPER(TRIM(o.car_model)), '_', COALESCE(b.CAR_BRAND_ID, -1))
        );
END;
$$;
 
 
CREATE OR REPLACE PROCEDURE BL_CL.LOAD_CE_CAR_MODELS()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_affected INTEGER := 0;
    v_default_row_count INTEGER := 0;
    rec RECORD;
BEGIN
	BEGIN
    INSERT INTO BL_3NF.CE_CAR_MODELS (CAR_MODEL_ID, MODEL_SRC_ID, CAR_BRAND_ID, MODEL_NAME, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT)
    SELECT -1, 'n. a.', -1, 'n. a.', 'MANUAL', 'DEFAULT_ROW', CURRENT_DATE  
    WHERE NOT EXISTS (SELECT 1 FROM BL_3NF.CE_CAR_MODELS WHERE CAR_MODEL_ID = -1);
 
    GET DIAGNOSTICS v_default_row_count = ROW_COUNT;
 
    FOR rec IN 
        SELECT DISTINCT 
            model_src_id, car_brand_id, model_name, source_system, source_entity
        FROM BL_CL.FN_GET_CAR_MODELS_TO_LOAD() f
        WHERE NOT EXISTS (
            SELECT 1 FROM BL_3NF.CE_CAR_MODELS cm 
            WHERE cm.MODEL_SRC_ID = f.model_src_id
        )
    LOOP
        INSERT INTO BL_3NF.CE_CAR_MODELS (
            CAR_MODEL_ID, MODEL_SRC_ID, CAR_BRAND_ID, MODEL_NAME, 
            SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT
        )
        VALUES (
            NEXTVAL('bl_3nf.ce_car_models_seq'),
            rec.model_src_id,
            rec.car_brand_id,
            rec.model_name,
            rec.source_system,
            rec.source_entity,
            CURRENT_DATE
        );
        v_rows_affected := v_rows_affected + 1;
    END LOOP;
 
    CALL BL_CL.LOG_PROCEDURE(
        'LOAD_CE_CAR_MODELS',
        v_rows_affected + v_default_row_count,
        'Successfully loaded car models. New rows: ' || v_rows_affected || ', Default rows: ' || v_default_row_count,
        'Success'
    );
 
EXCEPTION
    WHEN OTHERS THEN
        CALL BL_CL.LOG_PROCEDURE(
            'LOAD_CE_CAR_MODELS',
            -1,
            'Error: ' || SQLERRM,
            'Error'
        );
        RAISE;
	END;
END;
$$;
 
CALL BL_CL.LOAD_CE_CAR_MODELS();
 
SELECT * FROM BL_CL.LOG_TABLE 
WHERE procedure_name = 'LOAD_CE_CAR_MODELS' 
ORDER BY execution_time DESC;
 
SELECT COUNT(*) FROM BL_3NF.CE_CAR_MODELS;
SELECT * FROM BL_3NF.CE_CAR_MODELS order by model_name;
 