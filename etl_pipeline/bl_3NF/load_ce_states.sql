CREATE OR REPLACE FUNCTION BL_CL.FN_GET_STATES_TO_LOAD()
RETURNS TABLE (
    state_src_id VARCHAR(200),
    country_id INTEGER,
    state_name VARCHAR(100),
    source_system VARCHAR(50),
    source_entity VARCHAR(50)
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT
        CONCAT(UPPER(TRIM(o.customerstate)), '_', COALESCE(c.COUNTRY_ID, -1))::VARCHAR(200) AS state_src_id,
        COALESCE(c.COUNTRY_ID, -1) AS country_id,
        UPPER(TRIM(o.customerstate))::VARCHAR(100) AS state_name,
        'SA_CAR_SALES_OFFLINE'::VARCHAR(50) AS source_system,
        'SRC_CAR_SALES_OFFLINE'::VARCHAR(50) AS source_entity
    FROM sa_car_sales_offline.src_car_sales_offline o
    LEFT JOIN BL_3NF.CE_COUNTRIES c ON c.COUNTRY_SRC_ID = UPPER(TRIM(o.customercountry))
        AND c.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    WHERE TRIM(o.customerstate) IS NOT NULL
        AND TRIM(o.customerstate) != ''
        AND TRIM(o.customercountry) IS NOT NULL
        AND TRIM(o.customercountry) != ''
 
    UNION ALL
 
    SELECT DISTINCT
        CONCAT(UPPER(TRIM(o.customer_state)), '_', COALESCE(c.COUNTRY_ID, -1))::VARCHAR(200) AS state_src_id,
        COALESCE(c.COUNTRY_ID, -1) AS country_id,
        UPPER(TRIM(o.customer_state))::VARCHAR(100) AS state_name,
        'SA_CAR_SALES_ONLINE'::VARCHAR(50) AS source_system,
        'SRC_CAR_SALES_ONLINE'::VARCHAR(50) AS source_entity
    FROM sa_car_sales_online.src_car_sales_online o
    LEFT JOIN BL_3NF.CE_COUNTRIES c ON c.COUNTRY_SRC_ID = UPPER(TRIM(o.customer_country))
        AND c.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    WHERE TRIM(o.customer_state) IS NOT NULL
        AND TRIM(o.customer_state) != ''
        AND TRIM(o.customer_country) IS NOT NULL
        AND TRIM(o.customer_country) != ''
        AND NOT EXISTS (
            SELECT 1 FROM sa_car_sales_offline.src_car_sales_offline o2
            LEFT JOIN BL_3NF.CE_COUNTRIES c2 ON c2.COUNTRY_SRC_ID = UPPER(TRIM(o2.customercountry))
                AND c2.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
            WHERE CONCAT(UPPER(TRIM(o2.customerstate)), '_', COALESCE(c2.COUNTRY_ID, -1)) 
                  = CONCAT(UPPER(TRIM(o.customer_state)), '_', COALESCE(c.COUNTRY_ID, -1))
        );
END;
$$;
CREATE OR REPLACE PROCEDURE BL_CL.LOAD_CE_STATES()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_affected INTEGER := 0;
    v_default_row_count INTEGER := 0;
    rec RECORD;
BEGIN
	BEGIN
    INSERT INTO BL_3NF.CE_STATES (STATE_ID, STATE_SRC_ID, COUNTRY_ID, STATE_NAME, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT)
    SELECT -1, 'n. a.', -1, 'n. a.', 'MANUAL', 'DEFAULT_ROW', CURRENT_DATE 
    WHERE NOT EXISTS (SELECT 1 FROM BL_3NF.CE_STATES WHERE STATE_ID = -1);
 
    GET DIAGNOSTICS v_default_row_count = ROW_COUNT;
 
    FOR rec IN 
        SELECT DISTINCT 
            state_src_id, country_id, state_name, source_system, source_entity
        FROM BL_CL.FN_GET_STATES_TO_LOAD() f
        WHERE NOT EXISTS (
            SELECT 1 FROM BL_3NF.CE_STATES s 
            WHERE s.STATE_SRC_ID = f.state_src_id
        )
    LOOP
        INSERT INTO BL_3NF.CE_STATES (
            STATE_ID, STATE_SRC_ID, COUNTRY_ID, STATE_NAME, 
            SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT
        )
        VALUES (
            NEXTVAL('bl_3nf.ce_states_seq'),
            rec.state_src_id,
            rec.country_id,
            rec.state_name,
            rec.source_system,
            rec.source_entity,
            CURRENT_DATE
        );
        v_rows_affected := v_rows_affected + 1;
    END LOOP;
 
    CALL BL_CL.LOG_PROCEDURE(
        'LOAD_CE_STATES',
        v_rows_affected + v_default_row_count,
        'Successfully loaded states. New rows: ' || v_rows_affected || ', Default rows: ' || v_default_row_count,
        'Success'
    );
 
EXCEPTION
    WHEN OTHERS THEN
        CALL BL_CL.LOG_PROCEDURE(
            'LOAD_CE_STATES',
            -1,
            'Error: ' || SQLERRM,
            'Error'
        );
        RAISE;
	END;
END;
$$;
 
CALL BL_CL.LOAD_CE_STATES();
 
SELECT * FROM BL_CL.LOG_TABLE 
WHERE procedure_name = 'LOAD_CE_STATES' 
ORDER BY execution_time DESC;
 
SELECT * FROM BL_3NF.CE_STATES;