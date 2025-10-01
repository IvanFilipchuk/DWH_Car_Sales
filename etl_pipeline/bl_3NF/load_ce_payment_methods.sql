CREATE OR REPLACE FUNCTION BL_CL.FN_GET_PAYMENT_METHODS_TO_LOAD()
RETURNS TABLE (
    payment_method_src_id VARCHAR(100),
    method_name VARCHAR(100),
    source_system VARCHAR(50),
    source_entity VARCHAR(50)
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT
        UPPER(TRIM(paymentmethod))::VARCHAR(100) AS payment_method_src_id,
        UPPER(TRIM(paymentmethod))::VARCHAR(100) AS method_name,
        'SA_CAR_SALES_OFFLINE'::VARCHAR(50) AS source_system,
        'SRC_CAR_SALES_OFFLINE'::VARCHAR(50) AS source_entity
    FROM sa_car_sales_offline.src_car_sales_offline
    WHERE TRIM(paymentmethod) IS NOT NULL
        AND TRIM(paymentmethod) != ''
 
    UNION ALL
 
    SELECT DISTINCT
        UPPER(TRIM(payment_method))::VARCHAR(100) AS payment_method_src_id,
        UPPER(TRIM(payment_method))::VARCHAR(100) AS method_name,
        'SA_CAR_SALES_ONLINE'::VARCHAR(50) AS source_system,
        'SRC_CAR_SALES_ONLINE'::VARCHAR(50) AS source_entity
    FROM sa_car_sales_online.src_car_sales_online
    WHERE TRIM(payment_method) IS NOT NULL
        AND TRIM(payment_method) != ''
        AND NOT EXISTS (
            SELECT 1 FROM sa_car_sales_offline.src_car_sales_offline 
            WHERE UPPER(TRIM(paymentmethod)) = UPPER(TRIM(payment_method))
        );
END;
$$;
 
CREATE OR REPLACE PROCEDURE BL_CL.LOAD_CE_PAYMENT_METHODS()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_affected INTEGER := 0;
    v_default_row_count INTEGER := 0;
    rec RECORD;
BEGIN
	BEGIN
    INSERT INTO BL_3NF.CE_PAYMENT_METHODS (PAYMENT_METHOD_ID, PAYMENT_METHOD_SRC_ID, METHOD_NAME, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT)
    SELECT -1, 'n. a.', 'n. a.', 'MANUAL', 'DEFAULT_ROW', CURRENT_DATE  
    WHERE NOT EXISTS (SELECT 1 FROM BL_3NF.CE_PAYMENT_METHODS WHERE PAYMENT_METHOD_ID = -1);
 
    GET DIAGNOSTICS v_default_row_count = ROW_COUNT;
 
    FOR rec IN 
        SELECT DISTINCT 
            payment_method_src_id, method_name, source_system, source_entity
        FROM BL_CL.FN_GET_PAYMENT_METHODS_TO_LOAD() f
        WHERE NOT EXISTS (
            SELECT 1 FROM BL_3NF.CE_PAYMENT_METHODS pm 
            WHERE pm.PAYMENT_METHOD_SRC_ID = f.payment_method_src_id
        )
    LOOP
        INSERT INTO BL_3NF.CE_PAYMENT_METHODS (
            PAYMENT_METHOD_ID, PAYMENT_METHOD_SRC_ID, METHOD_NAME, 
            SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT
        )
        VALUES (
            NEXTVAL('bl_3nf.ce_payment_methods_seq'),
            rec.payment_method_src_id,
            rec.method_name,
            rec.source_system,
            rec.source_entity,
            CURRENT_DATE
        );
        v_rows_affected := v_rows_affected + 1;
    END LOOP;
 
    CALL BL_CL.LOG_PROCEDURE(
        'LOAD_CE_PAYMENT_METHODS',
        v_rows_affected + v_default_row_count,
        'Successfully loaded payment methods. New rows: ' || v_rows_affected || ', Default rows: ' || v_default_row_count,
        'Success'
    );
 
EXCEPTION
    WHEN OTHERS THEN
        CALL BL_CL.LOG_PROCEDURE(
            'LOAD_CE_PAYMENT_METHODS',
            -1,
            'Error: ' || SQLERRM,
            'Error'
        );
        RAISE;
	END;
END;
$$;
 
CALL BL_CL.LOAD_CE_PAYMENT_METHODS();
 
SELECT * FROM BL_CL.LOG_TABLE 
WHERE procedure_name = 'LOAD_CE_PAYMENT_METHODS' 
ORDER BY execution_time DESC;
 
SELECT COUNT(*) FROM BL_3NF.CE_PAYMENT_METHODS;
SELECT * FROM BL_3NF.CE_PAYMENT_METHODS;