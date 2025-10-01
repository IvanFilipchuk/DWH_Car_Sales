CREATE OR REPLACE FUNCTION BL_CL.FN_GET_ADDRESSES_TO_LOAD()
RETURNS TABLE (
    address_src_id VARCHAR(300),
    street_id INTEGER,
    building_number VARCHAR(20),
    postal_code VARCHAR(20),
    source_system VARCHAR(50),
    source_entity VARCHAR(50)
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT
        CONCAT(
            UPPER(TRIM(o.customerstreetname)), '_', 
            COALESCE(st.STREET_ID, -1), '_',  
            UPPER(TRIM(COALESCE(o.customerbuildingnumber, 'n. a.'))), '_',
            UPPER(TRIM(COALESCE(o.customerpostalcode, 'n. a.')))
        )::VARCHAR(300) AS address_src_id,
        COALESCE(st.STREET_ID, -1) AS street_id,
        COALESCE(UPPER(TRIM(o.customerbuildingnumber)), 'n. a.')::VARCHAR(20) AS building_number,
        COALESCE(UPPER(TRIM(o.customerpostalcode)), 'n. a.')::VARCHAR(20) AS postal_code,
        'SA_CAR_SALES_OFFLINE'::VARCHAR(50) AS source_system,
        'SRC_CAR_SALES_OFFLINE'::VARCHAR(50) AS source_entity
    FROM sa_car_sales_offline.src_car_sales_offline o
    LEFT JOIN BL_3NF.CE_COUNTRIES c ON c.COUNTRY_SRC_ID = UPPER(TRIM(o.customercountry))
        AND c.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    LEFT JOIN BL_3NF.CE_STATES s ON s.STATE_SRC_ID = CONCAT(UPPER(TRIM(o.customerstate)), '_', COALESCE(c.COUNTRY_ID, -1))  
        AND s.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    LEFT JOIN BL_3NF.CE_CITIES ci ON ci.CITY_SRC_ID = CONCAT(UPPER(TRIM(o.customercity)), '_', COALESCE(s.STATE_ID, -1)) 
        AND ci.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    LEFT JOIN BL_3NF.CE_STREETS st ON st.STREET_SRC_ID = CONCAT(UPPER(TRIM(o.customerstreetname)), '_', COALESCE(ci.CITY_ID, -1))  
        AND st.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    WHERE TRIM(o.customerstreetname) IS NOT NULL
 
    UNION ALL
 
    SELECT DISTINCT
        CONCAT(
            UPPER(TRIM(o.customer_street_name)), '_', 
            COALESCE(st.STREET_ID, -1), '_', 
            UPPER(TRIM(COALESCE(o.customer_building_number, 'n. a.'))), '_',
            UPPER(TRIM(COALESCE(o.customer_postal_code, 'n. a.')))
        )::VARCHAR(300) AS address_src_id,
        COALESCE(st.STREET_ID, -1) AS street_id,
        COALESCE(UPPER(TRIM(o.customer_building_number)), 'n. a.')::VARCHAR(20) AS building_number,
        COALESCE(UPPER(TRIM(o.customer_postal_code)), 'n. a.')::VARCHAR(20) AS postal_code,
        'SA_CAR_SALES_ONLINE'::VARCHAR(50) AS source_system,
        'SRC_CAR_SALES_ONLINE'::VARCHAR(50) AS source_entity
    FROM sa_car_sales_online.src_car_sales_online o
    LEFT JOIN BL_3NF.CE_COUNTRIES c ON c.COUNTRY_SRC_ID = UPPER(TRIM(o.customer_country))
        AND c.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    LEFT JOIN BL_3NF.CE_STATES s ON s.STATE_SRC_ID = CONCAT(UPPER(TRIM(o.customer_state)), '_', COALESCE(c.COUNTRY_ID, -1)) 
        AND s.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    LEFT JOIN BL_3NF.CE_CITIES ci ON ci.CITY_SRC_ID = CONCAT(UPPER(TRIM(o.customer_city)), '_', COALESCE(s.STATE_ID, -1)) 
        AND ci.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    LEFT JOIN BL_3NF.CE_STREETS st ON st.STREET_SRC_ID = CONCAT(UPPER(TRIM(o.customer_street_name)), '_', COALESCE(ci.CITY_ID, -1))  
        AND st.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    WHERE TRIM(o.customer_street_name) IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM sa_car_sales_offline.src_car_sales_offline o2
            LEFT JOIN BL_3NF.CE_COUNTRIES c2 ON c2.COUNTRY_SRC_ID = UPPER(TRIM(o2.customercountry))
            LEFT JOIN BL_3NF.CE_STATES s2 ON s2.STATE_SRC_ID = CONCAT(UPPER(TRIM(o2.customerstate)), '_', COALESCE(c2.COUNTRY_ID, -1))  
            LEFT JOIN BL_3NF.CE_CITIES ci2 ON ci2.CITY_SRC_ID = CONCAT(UPPER(TRIM(o2.customercity)), '_', COALESCE(s2.STATE_ID, -1))  
            LEFT JOIN BL_3NF.CE_STREETS st2 ON st2.STREET_SRC_ID = CONCAT(UPPER(TRIM(o2.customerstreetname)), '_', COALESCE(ci2.CITY_ID, -1))
            WHERE CONCAT(
                UPPER(TRIM(o2.customerstreetname)), '_', 
                COALESCE(st2.STREET_ID, -1), '_',  
                UPPER(TRIM(COALESCE(o2.customerbuildingnumber, 'n. a.'))), '_',
                UPPER(TRIM(COALESCE(o2.customerpostalcode, 'n. a.')))
            ) = CONCAT(
                UPPER(TRIM(o.customer_street_name)), '_', 
                COALESCE(st.STREET_ID, -1), '_', 
                UPPER(TRIM(COALESCE(o.customer_building_number, 'n. a.'))), '_',
                UPPER(TRIM(COALESCE(o.customer_postal_code, 'n. a.')))
            )
        );
END;
$$;
 
 
CREATE OR REPLACE PROCEDURE BL_CL.LOAD_CE_ADDRESSES()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_affected INTEGER := 0;
    v_default_row_count INTEGER := 0;
    rec RECORD;
BEGIN
	BEGIN
    INSERT INTO BL_3NF.CE_ADDRESSES (ADDRESS_ID, ADDRESS_SRC_ID, STREET_ID, BUILDING_NUMBER, POSTAL_CODE, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT)
    SELECT -1, 'n. a.', -1, 'n. a.', 'n. a.', 'MANUAL', 'DEFAULT_ROW', CURRENT_DATE 
    WHERE NOT EXISTS (SELECT 1 FROM BL_3NF.CE_ADDRESSES WHERE ADDRESS_ID = -1);
 
    GET DIAGNOSTICS v_default_row_count = ROW_COUNT;
 
    FOR rec IN 
        SELECT DISTINCT 
            address_src_id, street_id, building_number, postal_code, source_system, source_entity
        FROM BL_CL.FN_GET_ADDRESSES_TO_LOAD() f
        WHERE NOT EXISTS (
            SELECT 1 FROM BL_3NF.CE_ADDRESSES a 
            WHERE a.ADDRESS_SRC_ID = f.address_src_id
        )
    LOOP
        INSERT INTO BL_3NF.CE_ADDRESSES (
            ADDRESS_ID, ADDRESS_SRC_ID, STREET_ID, BUILDING_NUMBER, POSTAL_CODE,
            SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT
        )
        VALUES (
            NEXTVAL('bl_3nf.ce_addresses_seq'),
            rec.address_src_id,
            rec.street_id,
            rec.building_number,
            rec.postal_code,
            rec.source_system,
            rec.source_entity,
            CURRENT_DATE
        );
        v_rows_affected := v_rows_affected + 1;
    END LOOP;
 
    CALL BL_CL.LOG_PROCEDURE(
        'LOAD_CE_ADDRESSES',
        v_rows_affected + v_default_row_count,
        'Successfully loaded addresses. New rows: ' || v_rows_affected || ', Default rows: ' || v_default_row_count,
        'Success'
    );
 
EXCEPTION
    WHEN OTHERS THEN
        CALL BL_CL.LOG_PROCEDURE(
            'LOAD_CE_ADDRESSES',
            -1,
            'Error: ' || SQLERRM,
            'Error'
        );
        RAISE;
	END;
END;
$$;
 
CALL BL_CL.LOAD_CE_ADDRESSES();
 
SELECT * FROM BL_CL.LOG_TABLE 
WHERE procedure_name = 'LOAD_CE_ADDRESSES' 
ORDER BY execution_time DESC;
 
SELECT * FROM BL_3NF.CE_ADDRESSES ;