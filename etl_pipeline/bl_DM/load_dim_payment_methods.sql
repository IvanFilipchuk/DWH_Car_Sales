DROP TYPE IF EXISTS BL_CL.DIM_PAYMENT_METHODS_TYPE CASCADE;
CREATE TYPE BL_CL.DIM_PAYMENT_METHODS_TYPE AS (
    PAYMENT_METHOD_ID INTEGER,
    PAYMENT_METHOD_SRC_ID VARCHAR(100),
    METHOD_NAME VARCHAR(100),
    SOURCE_SYSTEM VARCHAR(50),
    SOURCE_ENTITY VARCHAR(50)
);

CREATE OR REPLACE FUNCTION BL_CL.GET_DIM_PAYMENT_METHODS_DATA()
RETURNS SETOF BL_CL.DIM_PAYMENT_METHODS_TYPE
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        p.PAYMENT_METHOD_ID,
        p.PAYMENT_METHOD_SRC_ID,
        p.METHOD_NAME,
        'BL_3NF'::VARCHAR(50) AS SOURCE_SYSTEM,
        'CE_PAYMENT_METHODS'::VARCHAR(50) AS SOURCE_ENTITY
    FROM BL_3NF.CE_PAYMENT_METHODS p
    WHERE p.PAYMENT_METHOD_ID > 0;
END;
$$;


CREATE OR REPLACE PROCEDURE BL_CL.LOAD_DIM_PAYMENT_METHODS()
LANGUAGE plpgsql
AS $$
DECLARE
    cur_payments REFCURSOR;
    v_payment_record BL_CL.DIM_PAYMENT_METHODS_TYPE;
    v_sql TEXT;
    v_rows_inserted INTEGER := 0;
    v_rows_updated INTEGER := 0;
    v_default_row_count INTEGER := 0;
    v_exists_count INTEGER;
BEGIN
	BEGIN
    INSERT INTO BL_DM.DIM_PAYMENT_METHODS (
        PAYMENT_METHOD_SURR_ID, PAYMENT_METHOD_SRC_ID, METHOD_NAME, 
        SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT
    )
    SELECT -1, '-1', 'n. a.', 'MANUAL', 'DEFAULT_ROW', CURRENT_DATE
    WHERE NOT EXISTS (
        SELECT 1 FROM BL_DM.DIM_PAYMENT_METHODS WHERE PAYMENT_METHOD_SURR_ID = -1
    );

    GET DIAGNOSTICS v_default_row_count = ROW_COUNT;

    v_sql := 'SELECT * FROM BL_CL.GET_DIM_PAYMENT_METHODS_DATA()';
    
    OPEN cur_payments FOR EXECUTE v_sql;
    
    LOOP
        FETCH cur_payments INTO v_payment_record;
        EXIT WHEN NOT FOUND;
        
        SELECT COUNT(*) INTO v_exists_count 
        FROM BL_DM.DIM_PAYMENT_METHODS 
        WHERE PAYMENT_METHOD_SRC_ID = v_payment_record.PAYMENT_METHOD_ID::TEXT;
        
        IF v_exists_count = 0 THEN
            INSERT INTO BL_DM.DIM_PAYMENT_METHODS (
                PAYMENT_METHOD_SURR_ID, 
                PAYMENT_METHOD_SRC_ID,
                METHOD_NAME,
                INSERT_DT, 
                SOURCE_SYSTEM, 
                SOURCE_ENTITY
            )
            VALUES (
                NEXTVAL('bl_dm.dim_payment_methods_seq'),
                v_payment_record.PAYMENT_METHOD_ID::TEXT,
                v_payment_record.METHOD_NAME,
                CURRENT_DATE,
                v_payment_record.SOURCE_SYSTEM,
                v_payment_record.SOURCE_ENTITY
            );
            v_rows_inserted := v_rows_inserted + 1;
        ELSE
            UPDATE BL_DM.DIM_PAYMENT_METHODS 
            SET 
                METHOD_NAME = v_payment_record.METHOD_NAME,
                SOURCE_SYSTEM = v_payment_record.SOURCE_SYSTEM,
                SOURCE_ENTITY = v_payment_record.SOURCE_ENTITY,
                UPDATE_DT = CURRENT_DATE
            WHERE PAYMENT_METHOD_SRC_ID = v_payment_record.PAYMENT_METHOD_ID::TEXT
            AND (
                METHOD_NAME IS DISTINCT FROM v_payment_record.METHOD_NAME OR
                SOURCE_SYSTEM IS DISTINCT FROM v_payment_record.SOURCE_SYSTEM OR
                SOURCE_ENTITY IS DISTINCT FROM v_payment_record.SOURCE_ENTITY
            );
            
            GET DIAGNOSTICS v_rows_updated = ROW_COUNT;
        END IF;
    END LOOP;
    
    CLOSE cur_payments;

    CALL BL_CL.LOG_PROCEDURE(
        'LOAD_DIM_PAYMENT_METHODS',
        v_rows_inserted + v_rows_updated + v_default_row_count,
        'Successfully loaded DIM_PAYMENT_METHODS. Inserted: ' || v_rows_inserted || ', Updated: ' || v_rows_updated || ', Default rows: ' || v_default_row_count,
        'Success'
    );

EXCEPTION
    WHEN OTHERS THEN
        CALL BL_CL.LOG_PROCEDURE(
            'LOAD_DIM_PAYMENT_METHODS',
            -1,
            'Error: ' || SQLERRM,
            'Error'
        );
        RAISE;
	END;	
END;
$$;

CALL BL_CL.LOAD_DIM_PAYMENT_METHODS();

SELECT * FROM BL_CL.LOG_TABLE 
WHERE procedure_name = 'LOAD_DIM_PAYMENT_METHODS' 
ORDER BY execution_time DESC;

SELECT * FROM BL_DM.DIM_PAYMENT_METHODS;