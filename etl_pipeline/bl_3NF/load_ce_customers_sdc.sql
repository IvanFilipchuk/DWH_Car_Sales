CREATE OR REPLACE FUNCTION BL_CL.FN_GET_CUSTOMERS_SDC_TO_LOAD()
RETURNS TABLE (
    customer_src_id VARCHAR(300),
    customer_first_name VARCHAR(100),
    customer_last_name VARCHAR(100),
    customer_email VARCHAR(100),
    customer_telephone_number VARCHAR(30),
    address_id INTEGER,
    start_dt DATE,
    end_dt DATE,
    is_active CHAR(1),
    source_system VARCHAR(50),
    source_entity VARCHAR(50)
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT
        CONCAT(
            UPPER(TRIM(COALESCE(o.customername, 'n. a.'))), '_',
            UPPER(TRIM(COALESCE(o.customeremail, 'n. a.'))), '_',
            COALESCE(a.ADDRESS_ID, -1) 
        )::VARCHAR(300) AS customer_src_id,
        COALESCE(
            NULLIF(TRIM(SPLIT_PART(UPPER(TRIM(COALESCE(o.customername, 'n. a.'))), ' ', 1)), ''), 
            'n. a.'
        )::VARCHAR(100) AS customer_first_name,
        COALESCE(
            NULLIF(TRIM(SPLIT_PART(UPPER(TRIM(COALESCE(o.customername, 'n. a.'))), ' ', 2)), ''), 
            'n. a.'
        )::VARCHAR(100) AS customer_last_name,
        COALESCE(UPPER(TRIM(o.customeremail)), 'n. a.')::VARCHAR(100) AS customer_email,
        COALESCE(UPPER(TRIM(o.customertelephonenumber)), 'n. a.')::VARCHAR(30) AS customer_telephone_number,
        COALESCE(a.ADDRESS_ID, -1) AS address_id,
        CURRENT_DATE AS start_dt,
        '9999-12-31'::DATE AS end_dt,
        'Y'::CHAR(1) AS is_active,
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
    LEFT JOIN BL_3NF.CE_ADDRESSES a ON a.ADDRESS_SRC_ID = CONCAT(
        UPPER(TRIM(o.customerstreetname)), '_', 
        COALESCE(st.STREET_ID, -1), '_',  
        UPPER(TRIM(COALESCE(o.customerbuildingnumber, 'n. a.'))), '_',
        UPPER(TRIM(COALESCE(o.customerpostalcode, 'n. a.')))
    )
        AND a.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    WHERE o.customername IS NOT NULL
        AND TRIM(o.customername) != ''
 
    UNION ALL
 
    SELECT DISTINCT
        CONCAT(
            UPPER(TRIM(COALESCE(o.customer_name_and_surname, 'n. a.'))), '_',
            UPPER(TRIM(COALESCE(o.customer_email, 'n. a.'))), '_',
            COALESCE(a.ADDRESS_ID, -1) 
        )::VARCHAR(300) AS customer_src_id,
        COALESCE(
            NULLIF(TRIM(SPLIT_PART(UPPER(TRIM(COALESCE(o.customer_name_and_surname, 'n. a.'))), ' ', 1)), ''), 
            'n. a.'
        )::VARCHAR(100) AS customer_first_name,
        COALESCE(
            NULLIF(TRIM(SPLIT_PART(UPPER(TRIM(COALESCE(o.customer_name_and_surname, 'n. a.'))), ' ', 2)), ''), 
            'n. a.'
        )::VARCHAR(100) AS customer_last_name,
        COALESCE(UPPER(TRIM(o.customer_email)), 'n. a.')::VARCHAR(100) AS customer_email,
        COALESCE(UPPER(TRIM(o.customer_telephone_number)), 'n. a.')::VARCHAR(30) AS customer_telephone_number,
        COALESCE(a.ADDRESS_ID, -1) AS address_id,
        CURRENT_DATE AS start_dt,
        '9999-12-31'::DATE AS end_dt,
        'Y'::CHAR(1) AS is_active,
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
    LEFT JOIN BL_3NF.CE_ADDRESSES a ON a.ADDRESS_SRC_ID = CONCAT(
        UPPER(TRIM(o.customer_street_name)), '_', 
        COALESCE(st.STREET_ID, -1), '_',  
        UPPER(TRIM(COALESCE(o.customer_building_number, 'n. a.'))), '_',
        UPPER(TRIM(COALESCE(o.customer_postal_code, 'n. a.')))
    )
        AND a.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    WHERE o.customer_name_and_surname IS NOT NULL
        AND TRIM(o.customer_name_and_surname) != ''
        AND NOT EXISTS (
            SELECT 1 FROM sa_car_sales_offline.src_car_sales_offline o2
            LEFT JOIN BL_3NF.CE_COUNTRIES c2 ON c2.COUNTRY_SRC_ID = UPPER(TRIM(o2.customercountry))
            LEFT JOIN BL_3NF.CE_STATES s2 ON s2.STATE_SRC_ID = CONCAT(UPPER(TRIM(o2.customerstate)), '_', COALESCE(c2.COUNTRY_ID, -1)) 
            LEFT JOIN BL_3NF.CE_CITIES ci2 ON ci2.CITY_SRC_ID = CONCAT(UPPER(TRIM(o2.customercity)), '_', COALESCE(s2.STATE_ID, -1)) 
            LEFT JOIN BL_3NF.CE_STREETS st2 ON st2.STREET_SRC_ID = CONCAT(UPPER(TRIM(o2.customerstreetname)), '_', COALESCE(ci2.CITY_ID, -1))
            LEFT JOIN BL_3NF.CE_ADDRESSES a2 ON a2.ADDRESS_SRC_ID = CONCAT(
                UPPER(TRIM(o2.customerstreetname)), '_', 
                COALESCE(st2.STREET_ID, -1), '_',  
                UPPER(TRIM(COALESCE(o2.customerbuildingnumber, 'n. a.'))), '_',
                UPPER(TRIM(COALESCE(o2.customerpostalcode, 'n. a.')))
            )
            WHERE CONCAT(
                UPPER(TRIM(COALESCE(o2.customername, 'n. a.'))), '_',
                UPPER(TRIM(COALESCE(o2.customeremail, 'n. a.'))), '_',
                COALESCE(a2.ADDRESS_ID, -1)  
            ) = CONCAT(
                UPPER(TRIM(COALESCE(o.customer_name_and_surname, 'n. a.'))), '_',
                UPPER(TRIM(COALESCE(o.customer_email, 'n. a.'))), '_',
                COALESCE(a.ADDRESS_ID, -1)  
            )
        );
END;
$$;


CREATE OR REPLACE PROCEDURE BL_CL.LOAD_CE_CUSTOMERS_SDC()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_affected INTEGER := 0;
    v_default_row_count INTEGER := 0;
    v_insert_new_count INTEGER := 0;
    v_update_old_count INTEGER := 0;
    v_insert_new_version_count INTEGER := 0;
BEGIN
	BEGIN
    INSERT INTO BL_3NF.CE_CUSTOMERS_SDC (
        CUSTOMER_ID, CUSTOMER_SRC_ID, CUSTOMER_FIRST_NAME, CUSTOMER_LAST_NAME,
        CUSTOMER_EMAIL, CUSTOMER_TELEPHONE_NUMBER, ADDRESS_ID, START_DT, END_DT, IS_ACTIVE,
        SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT
    )
    SELECT -1, 'n. a.', 'n. a.', 'n. a.', 'n. a.', 'n. a.', -1, CURRENT_DATE, '9999-12-31'::DATE, 'Y', 'MANUAL', 'DEFAULT_ROW', CURRENT_DATE
    WHERE NOT EXISTS (SELECT 1 FROM BL_3NF.CE_CUSTOMERS_SDC WHERE CUSTOMER_ID = -1);

    GET DIAGNOSTICS v_default_row_count = ROW_COUNT;

    CREATE TEMP TABLE tmp_customers_source ON COMMIT DROP AS
    SELECT DISTINCT
        customer_src_id, customer_first_name, customer_last_name, customer_email,
        customer_telephone_number, address_id, start_dt, end_dt, is_active,
        source_system, source_entity
    FROM BL_CL.FN_GET_CUSTOMERS_SDC_TO_LOAD();

    INSERT INTO BL_3NF.CE_CUSTOMERS_SDC (
        CUSTOMER_ID, CUSTOMER_SRC_ID, CUSTOMER_FIRST_NAME, CUSTOMER_LAST_NAME,
        CUSTOMER_EMAIL, CUSTOMER_TELEPHONE_NUMBER, ADDRESS_ID, START_DT, END_DT, IS_ACTIVE,
        SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT
    )
    SELECT
        NEXTVAL('bl_3nf.ce_customers_sdc_seq'),
        s.customer_src_id,
        s.customer_first_name,
        s.customer_last_name,
        s.customer_email,
        s.customer_telephone_number,
        s.address_id,
        s.start_dt,
        s.end_dt,
        s.is_active,
        s.source_system,
        s.source_entity,
        CURRENT_DATE
    FROM tmp_customers_source s
    WHERE NOT EXISTS (
        SELECT 1 FROM BL_3NF.CE_CUSTOMERS_SDC c
        WHERE c.CUSTOMER_SRC_ID = s.customer_src_id
    );
    GET DIAGNOSTICS v_insert_new_count = ROW_COUNT;

    CREATE TEMP TABLE tmp_customers_to_update ON COMMIT DROP AS
    SELECT
        c.CUSTOMER_ID as existing_customer_id,
        s.customer_src_id,
        s.customer_first_name,
        s.customer_last_name,
        s.customer_email,
        s.customer_telephone_number,
        s.address_id,
        s.source_system,
        s.source_entity
    FROM tmp_customers_source s
    JOIN BL_3NF.CE_CUSTOMERS_SDC c
      ON c.CUSTOMER_SRC_ID = s.customer_src_id
     AND c.IS_ACTIVE = 'Y'
    WHERE (
         COALESCE(c.CUSTOMER_FIRST_NAME, 'n. a.') IS DISTINCT FROM COALESCE(s.customer_first_name, 'n. a.') OR
         COALESCE(c.CUSTOMER_LAST_NAME, 'n. a.') IS DISTINCT FROM COALESCE(s.customer_last_name, 'n. a.') OR
         COALESCE(c.CUSTOMER_EMAIL, 'n. a.') IS DISTINCT FROM COALESCE(s.customer_email, 'n. a.') OR
         COALESCE(c.CUSTOMER_TELEPHONE_NUMBER, 'n. a.') IS DISTINCT FROM COALESCE(s.customer_telephone_number, 'n. a.')
    );

    UPDATE BL_3NF.CE_CUSTOMERS_SDC c
    SET IS_ACTIVE = 'N',
        END_DT = CURRENT_DATE - 1
    FROM tmp_customers_to_update u
    WHERE c.CUSTOMER_ID = u.existing_customer_id;
    GET DIAGNOSTICS v_update_old_count = ROW_COUNT;

    INSERT INTO BL_3NF.CE_CUSTOMERS_SDC (
        CUSTOMER_ID, CUSTOMER_SRC_ID, CUSTOMER_FIRST_NAME, CUSTOMER_LAST_NAME,
        CUSTOMER_EMAIL, CUSTOMER_TELEPHONE_NUMBER, ADDRESS_ID, START_DT, END_DT, IS_ACTIVE,
        SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT
    )
    SELECT
        NEXTVAL('bl_3nf.ce_customers_sdc_seq'),
        u.customer_src_id,
        u.customer_first_name,
        u.customer_last_name,
        u.customer_email,
        u.customer_telephone_number,
        u.address_id,
        CURRENT_DATE,
        '9999-12-31'::DATE, 
        'Y', 
        u.source_system,
        u.source_entity,
        CURRENT_DATE
    FROM tmp_customers_to_update u;
    GET DIAGNOSTICS v_insert_new_version_count = ROW_COUNT;

    v_rows_affected := v_insert_new_count + v_update_old_count + v_insert_new_version_count;

    CALL BL_CL.LOG_PROCEDURE(
        'LOAD_CE_CUSTOMERS_SDC',
        v_rows_affected + v_default_row_count,
        'Successfully loaded customers SDC. ' ||
        'New customers: ' || v_insert_new_count || ', ' ||
        'SCD Old Updated: ' || v_update_old_count || ', ' ||
        'SCD New Versions: ' || v_insert_new_version_count || ', ' ||
        'Default rows: ' || v_default_row_count,
        'Success'
    );

EXCEPTION
    WHEN OTHERS THEN
        CALL BL_CL.LOG_PROCEDURE(
            'LOAD_CE_CUSTOMERS_SDC',
            -1,
            'Error: ' || SQLERRM,
            'Error'
        );
        RAISE;
	END;
END;
$$;

call BL_CL.LOAD_CE_CUSTOMERS_SDC();
SELECT * FROM BL_CL.LOG_TABLE 
WHERE procedure_name = 'LOAD_CE_CUSTOMERS_SDC' 
ORDER BY execution_time DESC;

SELECT * FROM BL_3NF.CE_CUSTOMERS_SDC;