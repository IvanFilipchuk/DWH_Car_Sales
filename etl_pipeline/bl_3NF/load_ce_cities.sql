CREATE OR REPLACE FUNCTION BL_CL.FN_GET_CITIES_TO_LOAD()
RETURNS TABLE (
    city_src_id VARCHAR(200),
    state_id INTEGER,
    city_name VARCHAR(100),
    source_system VARCHAR(50),
    source_entity VARCHAR(50)
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT
        CONCAT(UPPER(TRIM(o.customercity)), '_', COALESCE(s.STATE_ID, -1))::VARCHAR(200) AS city_src_id,
        COALESCE(s.STATE_ID, -1) AS state_id,
        UPPER(TRIM(o.customercity))::VARCHAR(100) AS city_name,
        'SA_CAR_SALES_OFFLINE'::VARCHAR(50) AS source_system,
        'SRC_CAR_SALES_OFFLINE'::VARCHAR(50) AS source_entity
    FROM sa_car_sales_offline.src_car_sales_offline o
    LEFT JOIN BL_3NF.CE_COUNTRIES c ON c.COUNTRY_SRC_ID = UPPER(TRIM(o.customercountry))
        AND c.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    LEFT JOIN BL_3NF.CE_STATES s ON s.STATE_SRC_ID = CONCAT(UPPER(TRIM(o.customerstate)), '_', COALESCE(c.COUNTRY_ID, -1))  
        AND s.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    WHERE TRIM(o.customercity) IS NOT NULL
        AND TRIM(o.customercity) != ''
        AND TRIM(o.customerstate) IS NOT NULL
        AND TRIM(o.customerstate) != ''
        AND TRIM(o.customercountry) IS NOT NULL
        AND TRIM(o.customercountry) != ''
 
    UNION ALL
 
    SELECT DISTINCT
        CONCAT(UPPER(TRIM(o.customer_city)), '_', COALESCE(s.STATE_ID, -1))::VARCHAR(200) AS city_src_id,
        COALESCE(s.STATE_ID, -1) AS state_id,
        UPPER(TRIM(o.customer_city))::VARCHAR(100) AS city_name,
        'SA_CAR_SALES_ONLINE'::VARCHAR(50) AS source_system,
        'SRC_CAR_SALES_ONLINE'::VARCHAR(50) AS source_entity
    FROM sa_car_sales_online.src_car_sales_online o
    LEFT JOIN BL_3NF.CE_COUNTRIES c ON c.COUNTRY_SRC_ID = UPPER(TRIM(o.customer_country))
        AND c.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    LEFT JOIN BL_3NF.CE_STATES s ON s.STATE_SRC_ID = CONCAT(UPPER(TRIM(o.customer_state)), '_', COALESCE(c.COUNTRY_ID, -1))
        AND s.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    WHERE TRIM(o.customer_city) IS NOT NULL
        AND TRIM(o.customer_city) != ''
        AND TRIM(o.customer_state) IS NOT NULL
        AND TRIM(o.customer_state) != ''
        AND TRIM(o.customer_country) IS NOT NULL
        AND TRIM(o.customer_country) != ''
        AND NOT EXISTS (
            SELECT 1 FROM sa_car_sales_offline.src_car_sales_offline o2
            LEFT JOIN BL_3NF.CE_COUNTRIES c2 ON c2.COUNTRY_SRC_ID = UPPER(TRIM(o2.customercountry))
            LEFT JOIN BL_3NF.CE_STATES s2 ON s2.STATE_SRC_ID = CONCAT(UPPER(TRIM(o2.customerstate)), '_', COALESCE(c2.COUNTRY_ID, -1)) 
            WHERE CONCAT(UPPER(TRIM(o2.customercity)), '_', COALESCE(s2.STATE_ID, -1)) 
                  = CONCAT(UPPER(TRIM(o.customer_city)), '_', COALESCE(s.STATE_ID, -1))
        );
END;
$$;
 
CREATE OR REPLACE PROCEDURE BL_CL.LOAD_CE_CITIES()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_affected INTEGER := 0;
    v_default_row_count INTEGER := 0;
    rec RECORD;
BEGIN
	BEGIN
    INSERT INTO BL_3NF.CE_CITIES (CITY_ID, CITY_SRC_ID, STATE_ID, CITY_NAME, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT)
    SELECT -1, 'n. a.', -1, 'n. a.', 'MANUAL', 'DEFAULT_ROW', CURRENT_DATE  
    WHERE NOT EXISTS (SELECT 1 FROM BL_3NF.CE_CITIES WHERE CITY_ID = -1);
 
    GET DIAGNOSTICS v_default_row_count = ROW_COUNT;
 
    FOR rec IN 
        SELECT DISTINCT 
            city_src_id, state_id, city_name, source_system, source_entity
        FROM BL_CL.FN_GET_CITIES_TO_LOAD() f
        WHERE NOT EXISTS (
            SELECT 1 FROM BL_3NF.CE_CITIES ci 
            WHERE ci.CITY_SRC_ID = f.city_src_id
        )
    LOOP
        INSERT INTO BL_3NF.CE_CITIES (
            CITY_ID, CITY_SRC_ID, STATE_ID, CITY_NAME, 
            SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT
        )
        VALUES (
            NEXTVAL('bl_3nf.ce_cities_seq'),
            rec.city_src_id,
            rec.state_id,
            rec.city_name,
            rec.source_system,
            rec.source_entity,
            CURRENT_DATE
        );
        v_rows_affected := v_rows_affected + 1;
    END LOOP;
 
    CALL BL_CL.LOG_PROCEDURE(
        'LOAD_CE_CITIES',
        v_rows_affected + v_default_row_count,
        'Successfully loaded cities. New rows: ' || v_rows_affected || ', Default rows: ' || v_default_row_count,
        'Success'
    );
 
EXCEPTION
    WHEN OTHERS THEN
        CALL BL_CL.LOG_PROCEDURE(
            'LOAD_CE_CITIES',
            -1,
            'Error: ' || SQLERRM,
            'Error'
        );
        RAISE;
	END;
END;
$$;
 
 
CALL BL_CL.LOAD_CE_CITIES();
 
SELECT * FROM BL_CL.LOG_TABLE 
WHERE procedure_name = 'LOAD_CE_CITIES' 
ORDER BY execution_time DESC;
 
SELECT * FROM BL_3NF.CE_CITIES order by city_name;
SELECT city_name, count(*) FROM BL_3NF.CE_CITIES GROUP by city_name;