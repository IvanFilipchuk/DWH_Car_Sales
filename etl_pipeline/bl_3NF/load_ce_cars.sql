CREATE OR REPLACE FUNCTION BL_CL.FN_GET_CARS_TO_LOAD()
RETURNS TABLE (
    car_src_id VARCHAR(300),
    car_model_id INTEGER,
    year INTEGER,
    source_system VARCHAR(50),
    source_entity VARCHAR(50)
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT
        CONCAT(
            UPPER(TRIM(o.carmake)), '_', 
            UPPER(TRIM(o.carmodel)), '_', 
            UPPER(TRIM(COALESCE(o.caryear, 'n. a.')))
        )::VARCHAR(300) AS car_src_id,
        COALESCE(m.CAR_MODEL_ID, -1) AS car_model_id,
        CASE 
            WHEN UPPER(TRIM(o.caryear)) ~ '^\d{4}$' 
                 AND UPPER(TRIM(o.caryear))::INT BETWEEN 1900 AND 2100
            THEN UPPER(TRIM(o.caryear))::INT
            ELSE -1
        END AS year,
        'SA_CAR_SALES_OFFLINE'::VARCHAR(50) AS source_system,
        'SRC_CAR_SALES_OFFLINE'::VARCHAR(50) AS source_entity
    FROM sa_car_sales_offline.src_car_sales_offline o
    LEFT JOIN BL_3NF.CE_CAR_BRANDS b ON b.BRAND_SRC_ID = UPPER(TRIM(o.carmake))
        AND b.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    LEFT JOIN BL_3NF.CE_CAR_MODELS m ON m.MODEL_SRC_ID = CONCAT(UPPER(TRIM(o.carmodel)), '_', COALESCE(b.CAR_BRAND_ID, -1)) 
        AND m.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    WHERE TRIM(o.carmake) IS NOT NULL 
        AND TRIM(o.carmodel) IS NOT NULL
 
    UNION ALL
 
    SELECT DISTINCT
        CONCAT(
            UPPER(TRIM(o.brand)), '_', 
            UPPER(TRIM(o.car_model)), '_', 
            UPPER(TRIM(COALESCE(o.car_year, 'n. a.')))
        )::VARCHAR(300) AS car_src_id,
        COALESCE(m.CAR_MODEL_ID, -1) AS car_model_id,
        CASE 
            WHEN UPPER(TRIM(o.car_year)) ~ '^\d{4}$' 
                 AND UPPER(TRIM(o.car_year))::INT BETWEEN 1900 AND 2100
            THEN UPPER(TRIM(o.car_year))::INT
            ELSE -1
        END AS year,
        'SA_CAR_SALES_ONLINE'::VARCHAR(50) AS source_system,
        'SRC_CAR_SALES_ONLINE'::VARCHAR(50) AS source_entity
    FROM sa_car_sales_online.src_car_sales_online o
    LEFT JOIN BL_3NF.CE_CAR_BRANDS b ON b.BRAND_SRC_ID = UPPER(TRIM(o.brand))
        AND b.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    LEFT JOIN BL_3NF.CE_CAR_MODELS m ON m.MODEL_SRC_ID = CONCAT(UPPER(TRIM(o.car_model)), '_', COALESCE(b.CAR_BRAND_ID, -1))
        AND m.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    WHERE TRIM(o.brand) IS NOT NULL 
        AND TRIM(o.car_model) IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM sa_car_sales_offline.src_car_sales_offline o2
            WHERE CONCAT(
                UPPER(TRIM(o2.carmake)), '_', 
                UPPER(TRIM(o2.carmodel)), '_', 
                UPPER(TRIM(COALESCE(o2.caryear, 'n. a.')))
            ) = CONCAT(
                UPPER(TRIM(o.brand)), '_', 
                UPPER(TRIM(o.car_model)), '_', 
                UPPER(TRIM(COALESCE(o.car_year, 'n. a.')))
            )
        );
END;
$$;
 
CREATE OR REPLACE PROCEDURE BL_CL.LOAD_CE_CARS()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_affected INTEGER := 0;
    v_default_row_count INTEGER := 0;
    rec RECORD;
BEGIN
	BEGIN
    INSERT INTO BL_3NF.CE_CARS (CAR_ID, CAR_SRC_ID, CAR_MODEL_ID, YEAR, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT)
    SELECT -1, 'n. a.', -1, -1, 'MANUAL', 'DEFAULT_ROW', CURRENT_DATE  
    WHERE NOT EXISTS (SELECT 1 FROM BL_3NF.CE_CARS WHERE CAR_ID = -1);
 
    GET DIAGNOSTICS v_default_row_count = ROW_COUNT;
 
    FOR rec IN 
        SELECT DISTINCT 
            car_src_id, car_model_id, year, source_system, source_entity
        FROM BL_CL.FN_GET_CARS_TO_LOAD() f
        WHERE NOT EXISTS (
            SELECT 1 FROM BL_3NF.CE_CARS c 
            WHERE c.CAR_SRC_ID = f.car_src_id
        )
    LOOP
        INSERT INTO BL_3NF.CE_CARS (
            CAR_ID, CAR_SRC_ID, CAR_MODEL_ID, YEAR, 
            SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT
        )
        VALUES (
            NEXTVAL('bl_3nf.ce_cars_seq'),
            rec.car_src_id,
            rec.car_model_id,
            rec.year,
            rec.source_system,
            rec.source_entity,
            CURRENT_DATE
        );
        v_rows_affected := v_rows_affected + 1;
    END LOOP;
 
    CALL BL_CL.LOG_PROCEDURE(
        'LOAD_CE_CARS',
        v_rows_affected + v_default_row_count,
        'Successfully loaded cars. New rows: ' || v_rows_affected || ', Default rows: ' || v_default_row_count,
        'Success'
    );
 
EXCEPTION
    WHEN OTHERS THEN
        CALL BL_CL.LOG_PROCEDURE(
            'LOAD_CE_CARS',
            -1,
            'Error: ' || SQLERRM,
            'Error'
        );
        RAISE;
	END;
END;
$$;
 
CALL BL_CL.LOAD_CE_CARS();
 
SELECT * FROM BL_CL.LOG_TABLE 
WHERE procedure_name = 'LOAD_CE_CARS' 
ORDER BY execution_time DESC;
 
SELECT * FROM BL_3NF.CE_CARS;