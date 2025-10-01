DROP TYPE IF EXISTS BL_CL.DIM_EMPLOYEES_TYPE CASCADE;
CREATE TYPE BL_CL.DIM_EMPLOYEES_TYPE AS (
    EMPLOYEE_ID INTEGER,
    EMPLOYEE_SRC_ID VARCHAR(100),
    EMPLOYEE_FIRST_NAME VARCHAR(100),
    EMPLOYEE_LAST_NAME VARCHAR(100),
    SOURCE_SYSTEM VARCHAR(50),
    SOURCE_ENTITY VARCHAR(50)
);

CREATE OR REPLACE FUNCTION BL_CL.GET_DIM_EMPLOYEES_DATA()
RETURNS SETOF BL_CL.DIM_EMPLOYEES_TYPE
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        e.EMPLOYEE_ID,
        e.EMPLOYEE_SRC_ID,
        e.EMPLOYEE_FIRST_NAME,
        e.EMPLOYEE_LAST_NAME,
        'BL_3NF'::VARCHAR(50) AS SOURCE_SYSTEM,
        'CE_EMPLOYEES'::VARCHAR(50) AS SOURCE_ENTITY
    FROM BL_3NF.CE_EMPLOYEES e
    WHERE e.EMPLOYEE_ID > 0;
END;
$$;

CREATE OR REPLACE PROCEDURE BL_CL.LOAD_DIM_EMPLOYEES()
LANGUAGE plpgsql
AS $$
DECLARE
    cur_employees REFCURSOR;
    v_employee_record BL_CL.DIM_EMPLOYEES_TYPE;
    v_sql TEXT;
    v_rows_inserted INTEGER := 0;
    v_rows_updated INTEGER := 0;
    v_default_row_count INTEGER := 0;
    v_exists_count INTEGER;
BEGIN
	BEGIN
    INSERT INTO BL_DM.DIM_EMPLOYEES (
        EMPLOYEE_SURR_ID, EMPLOYEE_SRC_ID, EMPLOYEE_FIRST_NAME, EMPLOYEE_LAST_NAME, 
        INSERT_DT, SOURCE_SYSTEM, SOURCE_ENTITY
    )
    SELECT -1, '-1', 'n. a.', 'n. a.', CURRENT_DATE, 'MANUAL', 'DEFAULT_ROW'
    WHERE NOT EXISTS (
        SELECT 1 FROM BL_DM.DIM_EMPLOYEES WHERE EMPLOYEE_SURR_ID = -1
    );

    GET DIAGNOSTICS v_default_row_count = ROW_COUNT;

    v_sql := 'SELECT * FROM BL_CL.GET_DIM_EMPLOYEES_DATA()';
    
    OPEN cur_employees FOR EXECUTE v_sql;
    
    LOOP
        FETCH cur_employees INTO v_employee_record;
        EXIT WHEN NOT FOUND;
        
        SELECT COUNT(*) INTO v_exists_count 
        FROM BL_DM.DIM_EMPLOYEES 
        WHERE EMPLOYEE_SRC_ID = v_employee_record.EMPLOYEE_ID::TEXT;
        
        IF v_exists_count = 0 THEN
           
            INSERT INTO BL_DM.DIM_EMPLOYEES (
                EMPLOYEE_SURR_ID, 
                EMPLOYEE_SRC_ID,     
                EMPLOYEE_FIRST_NAME,
                EMPLOYEE_LAST_NAME,
                INSERT_DT, 
                SOURCE_SYSTEM, 
                SOURCE_ENTITY
            )
            VALUES (
                NEXTVAL('bl_dm.dim_employees_seq'),
                v_employee_record.EMPLOYEE_ID::TEXT,  
                v_employee_record.EMPLOYEE_FIRST_NAME,
                v_employee_record.EMPLOYEE_LAST_NAME,
                CURRENT_DATE,
                v_employee_record.SOURCE_SYSTEM,
                v_employee_record.SOURCE_ENTITY
            );
            v_rows_inserted := v_rows_inserted + 1;
        ELSE
            UPDATE BL_DM.DIM_EMPLOYEES 
            SET 
                EMPLOYEE_FIRST_NAME = v_employee_record.EMPLOYEE_FIRST_NAME,
                EMPLOYEE_LAST_NAME = v_employee_record.EMPLOYEE_LAST_NAME,
                SOURCE_SYSTEM = v_employee_record.SOURCE_SYSTEM,
                SOURCE_ENTITY = v_employee_record.SOURCE_ENTITY,
                UPDATE_DT = CURRENT_DATE
            WHERE EMPLOYEE_SRC_ID = v_employee_record.EMPLOYEE_ID::TEXT
            AND (
                EMPLOYEE_FIRST_NAME IS DISTINCT FROM v_employee_record.EMPLOYEE_FIRST_NAME OR
                EMPLOYEE_LAST_NAME IS DISTINCT FROM v_employee_record.EMPLOYEE_LAST_NAME OR
                SOURCE_SYSTEM IS DISTINCT FROM v_employee_record.SOURCE_SYSTEM OR
                SOURCE_ENTITY IS DISTINCT FROM v_employee_record.SOURCE_ENTITY
            );
            
            GET DIAGNOSTICS v_rows_updated = ROW_COUNT;
        END IF;
    END LOOP;
    
    CLOSE cur_employees;

    CALL BL_CL.LOG_PROCEDURE(
        'LOAD_DIM_EMPLOYEES',
        v_rows_inserted + v_rows_updated + v_default_row_count,
        'Successfully loaded DIM_EMPLOYEES. Inserted: ' || v_rows_inserted || ', Updated: ' || v_rows_updated || ', Default rows: ' || v_default_row_count,
        'Success'
    );

EXCEPTION
    WHEN OTHERS THEN
        CALL BL_CL.LOG_PROCEDURE(
            'LOAD_DIM_EMPLOYEES',
            -1,
            'Error: ' || SQLERRM,
            'Error'
        );
        RAISE;
	END;
END;
$$;


CALL BL_CL.LOAD_DIM_EMPLOYEES();

SELECT * FROM BL_CL.LOG_TABLE 
WHERE procedure_name = 'LOAD_DIM_EMPLOYEES' 
ORDER BY execution_time DESC;

SELECT * FROM BL_DM.DIM_EMPLOYEES ;