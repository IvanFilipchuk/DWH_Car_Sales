CREATE OR REPLACE FUNCTION BL_CL.FN_GET_STREETS_TO_LOAD()
RETURNS TABLE (
    street_src_id VARCHAR(200),
    city_id INTEGER,
    street_name VARCHAR(100),
    source_system VARCHAR(50),
    source_entity VARCHAR(50)
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT
        CONCAT(UPPER(TRIM(o.customerstreetname)), '_', COALESCE(ci.CITY_ID, -1))::VARCHAR(200) AS street_src_id,
        COALESCE(ci.CITY_ID, -1) AS city_id,
        UPPER(TRIM(o.customerstreetname))::VARCHAR(100) AS street_name,
        'SA_CAR_SALES_OFFLINE'::VARCHAR(50) AS source_system,
        'SRC_CAR_SALES_OFFLINE'::VARCHAR(50) AS source_entity
    FROM sa_car_sales_offline.src_car_sales_offline o
    LEFT JOIN BL_3NF.CE_COUNTRIES c ON c.COUNTRY_SRC_ID = UPPER(TRIM(o.customercountry))
        AND c.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    LEFT JOIN BL_3NF.CE_STATES s ON s.STATE_SRC_ID = CONCAT(UPPER(TRIM(o.customerstate)), '_', COALESCE(c.COUNTRY_ID, -1))  
        AND s.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    LEFT JOIN BL_3NF.CE_CITIES ci ON ci.CITY_SRC_ID = CONCAT(UPPER(TRIM(o.customercity)), '_', COALESCE(s.STATE_ID, -1)) 
        AND ci.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    WHERE TRIM(o.customerstreetname) IS NOT NULL
        AND TRIM(o.customerstreetname) != ''
        AND TRIM(o.customercity) IS NOT NULL
        AND TRIM(o.customercity) != ''
        AND TRIM(o.customerstate) IS NOT NULL
        AND TRIM(o.customerstate) != ''
        AND TRIM(o.customercountry) IS NOT NULL
        AND TRIM(o.customercountry) != ''
 
    UNION ALL
 
    SELECT DISTINCT
        CONCAT(UPPER(TRIM(o.customer_street_name)), '_', COALESCE(ci.CITY_ID, -1))::VARCHAR(200) AS street_src_id,
        COALESCE(ci.CITY_ID, -1) AS city_id,
        UPPER(TRIM(o.customer_street_name))::VARCHAR(100) AS street_name,
        'SA_CAR_SALES_ONLINE'::VARCHAR(50) AS source_system,
        'SRC_CAR_SALES_ONLINE'::VARCHAR(50) AS source_entity
    FROM sa_car_sales_online.src_car_sales_online o
    LEFT JOIN BL_3NF.CE_COUNTRIES c ON c.COUNTRY_SRC_ID = UPPER(TRIM(o.customer_country))
        AND c.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    LEFT JOIN BL_3NF.CE_STATES s ON s.STATE_SRC_ID = CONCAT(UPPER(TRIM(o.customer_state)), '_', COALESCE(c.COUNTRY_ID, -1))
        AND s.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    LEFT JOIN BL_3NF.CE_CITIES ci ON ci.CITY_SRC_ID = CONCAT(UPPER(TRIM(o.customer_city)), '_', COALESCE(s.STATE_ID, -1))  
        AND ci.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    WHERE TRIM(o.customer_street_name) IS NOT NULL
        AND TRIM(o.customer_street_name) != ''
        AND TRIM(o.customer_city) IS NOT NULL
        AND TRIM(o.customer_city) != ''
        AND TRIM(o.customer_state) IS NOT NULL
        AND TRIM(o.customer_state) != ''
        AND TRIM(o.customer_country) IS NOT NULL
        AND TRIM(o.customer_country) != ''
        AND NOT EXISTS (
            SELECT 1 FROM sa_car_sales_offline.src_car_sales_offline o2
            LEFT JOIN BL_3NF.CE_COUNTRIES c2 ON c2.COUNTRY_SRC_ID = UPPER(TRIM(o2.customercountry))
            LEFT JOIN BL_3NF.CE_STATES s2 ON s2.STATE_SRC_ID = CONCAT(UPPER(TRIM(o2.customerstate)), '_', COALESCE(c2.COUNTRY_ID, -1)) 
            LEFT JOIN BL_3NF.CE_CITIES ci2 ON ci2.CITY_SRC_ID = CONCAT(UPPER(TRIM(o2.customercity)), '_', COALESCE(s2.STATE_ID, -1)) 
            WHERE CONCAT(UPPER(TRIM(o2.customerstreetname)), '_', COALESCE(ci2.CITY_ID, -1))
                  = CONCAT(UPPER(TRIM(o.customer_street_name)), '_', COALESCE(ci.CITY_ID, -1))
        );
END;
$$;
 
CREATE OR REPLACE PROCEDURE BL_CL.LOAD_CE_STREETS()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_affected INTEGER := 0;
    v_default_row_count INTEGER := 0;
    rec RECORD;
BEGIN
	BEGIN
    INSERT INTO BL_3NF.CE_STREETS (STREET_ID, STREET_SRC_ID, CITY_ID, STREET_NAME, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT)
    SELECT -1, 'n. a.', -1, 'n. a.', 'MANUAL', 'DEFAULT_ROW', CURRENT_DATE 
    WHERE NOT EXISTS (SELECT 1 FROM BL_3NF.CE_STREETS WHERE STREET_ID = -1);
 
    GET DIAGNOSTICS v_default_row_count = ROW_COUNT;
 
    FOR rec IN 
        SELECT DISTINCT 
            street_src_id, city_id, street_name, source_system, source_entity
        FROM BL_CL.FN_GET_STREETS_TO_LOAD() f
        WHERE NOT EXISTS (
            SELECT 1 FROM BL_3NF.CE_STREETS st 
            WHERE st.STREET_SRC_ID = f.street_src_id
        )
    LOOP
        INSERT INTO BL_3NF.CE_STREETS (
            STREET_ID, STREET_SRC_ID, CITY_ID, STREET_NAME, 
            SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT
        )
        VALUES (
            NEXTVAL('bl_3nf.ce_streets_seq'),
            rec.street_src_id,
            rec.city_id,
            rec.street_name,
            rec.source_system,
            rec.source_entity,
            CURRENT_DATE
        );
        v_rows_affected := v_rows_affected + 1;
    END LOOP;
 
    CALL BL_CL.LOG_PROCEDURE(
        'LOAD_CE_STREETS',
        v_rows_affected + v_default_row_count,
        'Successfully loaded streets. New rows: ' || v_rows_affected || ', Default rows: ' || v_default_row_count,
        'Success'
    );
 
EXCEPTION
    WHEN OTHERS THEN
        CALL BL_CL.LOG_PROCEDURE(
            'LOAD_CE_STREETS',
            -1,
            'Error: ' || SQLERRM,
            'Error'
        );
        RAISE;
	END;
END;
$$;
 
CALL BL_CL.LOAD_CE_STREETS();
 
SELECT * FROM BL_CL.LOG_TABLE 
WHERE procedure_name = 'LOAD_CE_STREETS' 
ORDER BY execution_time DESC;
 
SELECT COUNT(*) FROM BL_3NF.CE_STREETS;
SELECT * FROM BL_3NF.CE_STREETS ORDER BY street_name;