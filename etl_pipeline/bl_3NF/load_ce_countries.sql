CREATE OR REPLACE FUNCTION BL_CL.FN_GET_COUNTRIES_TO_LOAD()
RETURNS TABLE (
    country_src VARCHAR(100),
    country_name VARCHAR(100),
    source_system VARCHAR(50),
    source_entity VARCHAR(50)
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT
        UPPER(TRIM(customercountry))::VARCHAR(100) as country_src,
        UPPER(TRIM(customercountry))::VARCHAR(100) as country_name,
        'SA_CAR_SALES_OFFLINE'::VARCHAR(50) as source_system,
        'SRC_CAR_SALES_OFFLINE'::VARCHAR(50) as source_entity
    FROM sa_car_sales_offline.src_car_sales_offline
    WHERE TRIM(customercountry) IS NOT NULL 
        AND TRIM(customercountry) != ''
 
    UNION ALL
 
    SELECT DISTINCT
        UPPER(TRIM(customer_country))::VARCHAR(100) as country_src,
        UPPER(TRIM(customer_country))::VARCHAR(100) as country_name,
        'SA_CAR_SALES_ONLINE'::VARCHAR(50) as source_system,
        'SRC_CAR_SALES_ONLINE'::VARCHAR(50) as source_entity
    FROM sa_car_sales_online.src_car_sales_online
    WHERE TRIM(customer_country) IS NOT NULL 
        AND TRIM(customer_country) != ''
        AND NOT EXISTS (
            SELECT 1 FROM sa_car_sales_offline.src_car_sales_offline 
            WHERE UPPER(TRIM(customercountry)) = UPPER(TRIM(customer_country))
        );
END;
$$;
 
 
CREATE OR REPLACE PROCEDURE BL_CL.LOAD_CE_COUNTRIES()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_affected INTEGER := 0;
    v_default_row_count INTEGER := 0;
    rec RECORD;
BEGIN
	BEGIN
    INSERT INTO BL_3NF.CE_COUNTRIES (COUNTRY_ID, COUNTRY_SRC_ID, COUNTRY_NAME, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT)
    SELECT -1, 'n. a.', 'n. a.', 'MANUAL', 'DEFAULT_ROW', CURRENT_DATE  
    WHERE NOT EXISTS (SELECT 1 FROM BL_3NF.CE_COUNTRIES WHERE COUNTRY_ID = -1);
 
    GET DIAGNOSTICS v_default_row_count = ROW_COUNT;
 
    FOR rec IN 
        SELECT DISTINCT 
            country_src, country_name, source_system, source_entity
        FROM BL_CL.FN_GET_COUNTRIES_TO_LOAD() f
        WHERE NOT EXISTS (
            SELECT 1 FROM BL_3NF.CE_COUNTRIES c 
            WHERE c.COUNTRY_SRC_ID = f.country_src
        )
    LOOP
        INSERT INTO BL_3NF.CE_COUNTRIES (
            COUNTRY_ID, COUNTRY_SRC_ID, COUNTRY_NAME, 
            SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT
        )
        VALUES (
            NEXTVAL('bl_3nf.ce_countries_seq'),
            rec.country_src,
            rec.country_name,
            rec.source_system,
            rec.source_entity,
            CURRENT_DATE
        );
        v_rows_affected := v_rows_affected + 1;
    END LOOP;
 
    CALL BL_CL.LOG_PROCEDURE(
        'LOAD_CE_COUNTRIES',
        v_rows_affected + v_default_row_count,
        'Successfully loaded countries. New rows: ' || v_rows_affected || ', Default rows: ' || v_default_row_count,
        'Success'
    );
 
EXCEPTION
    WHEN OTHERS THEN
        CALL BL_CL.LOG_PROCEDURE(
            'LOAD_CE_COUNTRIES',
            -1,
            'Error: ' || SQLERRM,
            'Error'
        );
        RAISE;
	END;
END;
$$;

CALL BL_CL.LOAD_CE_COUNTRIES();

SELECT * FROM BL_CL.LOG_TABLE 
WHERE procedure_name = 'LOAD_CE_COUNTRIES' 
ORDER BY execution_time DESC;

SELECT COUNT(*) FROM BL_3NF.CE_COUNTRIES;
SELECT * FROM BL_3NF.CE_COUNTRIES;