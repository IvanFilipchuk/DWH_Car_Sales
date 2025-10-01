CREATE OR REPLACE FUNCTION BL_CL.FN_GET_EMPLOYEES_TO_LOAD()
RETURNS TABLE (
    employee_src_id VARCHAR(100),
    employee_first_name VARCHAR(100),
    employee_last_name VARCHAR(100),
    source_system VARCHAR(50),
    source_entity VARCHAR(50)
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT
        UPPER(TRIM(COALESCE(salespersonid::TEXT, 'n. a.')))::VARCHAR(100) AS employee_src_id,
        COALESCE(
            NULLIF(TRIM(SPLIT_PART(UPPER(TRIM(COALESCE(salesperson, 'n. a.'))), ' ', 1)), ''), 
            'n. a.'
        )::VARCHAR(100) AS employee_first_name,
        COALESCE(
            NULLIF(TRIM(SPLIT_PART(UPPER(TRIM(COALESCE(salesperson, 'n. a.'))), ' ', 2)), ''), 
            'n. a.'
        )::VARCHAR(100) AS employee_last_name,
        'SA_CAR_SALES_OFFLINE'::VARCHAR(50) AS source_system,
        'SRC_CAR_SALES_OFFLINE'::VARCHAR(50) AS source_entity
    FROM sa_car_sales_offline.src_car_sales_offline
    WHERE salespersonid IS NOT NULL 
        AND TRIM(COALESCE(salespersonid::TEXT, '')) != ''
        AND salesperson IS NOT NULL
        AND TRIM(COALESCE(salesperson, '')) != ''
 
    UNION ALL
 
    SELECT DISTINCT
        UPPER(TRIM(COALESCE(employee_id::TEXT, 'n. a.')))::VARCHAR(100) AS employee_src_id,
        COALESCE(
            NULLIF(TRIM(SPLIT_PART(UPPER(TRIM(COALESCE(employee, 'n. a.'))), ' ', 1)), ''), 
            'n. a.'
        )::VARCHAR(100) AS employee_first_name,
        COALESCE(
            NULLIF(TRIM(SPLIT_PART(UPPER(TRIM(COALESCE(employee, 'n. a.'))), ' ', 2)), ''), 
            'n. a.'
        )::VARCHAR(100) AS employee_last_name,
        'SA_CAR_SALES_ONLINE'::VARCHAR(50) AS source_system,
        'SRC_CAR_SALES_ONLINE'::VARCHAR(50) AS source_entity
    FROM sa_car_sales_online.src_car_sales_online
    WHERE employee_id IS NOT NULL 
        AND TRIM(COALESCE(employee_id::TEXT, '')) != ''
        AND employee IS NOT NULL
        AND TRIM(COALESCE(employee, '')) != ''
        AND NOT EXISTS (
            SELECT 1 FROM sa_car_sales_offline.src_car_sales_offline 
            WHERE UPPER(TRIM(COALESCE(salespersonid::TEXT, 'n. a.'))) = UPPER(TRIM(COALESCE(employee_id::TEXT, 'n. a.')))
        );
END;
$$;
 
CREATE OR REPLACE PROCEDURE BL_CL.LOAD_CE_EMPLOYEES()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_affected INTEGER := 0;
    v_default_row_count INTEGER := 0;
    rec RECORD;
BEGIN
	BEGIN
    INSERT INTO BL_3NF.CE_EMPLOYEES (EMPLOYEE_ID, EMPLOYEE_SRC_ID, EMPLOYEE_FIRST_NAME, EMPLOYEE_LAST_NAME, SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT)
    SELECT -1, 'n. a.', 'n. a.', 'n. a.', 'MANUAL', 'DEFAULT_ROW', CURRENT_DATE  
    WHERE NOT EXISTS (SELECT 1 FROM BL_3NF.CE_EMPLOYEES WHERE EMPLOYEE_ID = -1);
 
    GET DIAGNOSTICS v_default_row_count = ROW_COUNT;
 
    FOR rec IN 
        SELECT DISTINCT 
            employee_src_id, employee_first_name, employee_last_name, source_system, source_entity
        FROM BL_CL.FN_GET_EMPLOYEES_TO_LOAD() f
        WHERE NOT EXISTS (
            SELECT 1 FROM BL_3NF.CE_EMPLOYEES e 
            WHERE e.EMPLOYEE_SRC_ID = f.employee_src_id
        )
    LOOP
        INSERT INTO BL_3NF.CE_EMPLOYEES (
            EMPLOYEE_ID, EMPLOYEE_SRC_ID, EMPLOYEE_FIRST_NAME, EMPLOYEE_LAST_NAME, 
            SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT
        )
        VALUES (
            NEXTVAL('bl_3nf.ce_employees_seq'),
            rec.employee_src_id,
            rec.employee_first_name,
            rec.employee_last_name,
            rec.source_system,
            rec.source_entity,
            CURRENT_DATE
        );
        v_rows_affected := v_rows_affected + 1;
    END LOOP;
 
    CALL BL_CL.LOG_PROCEDURE(
        'LOAD_CE_EMPLOYEES',
        v_rows_affected + v_default_row_count,
        'Successfully loaded employees. New rows: ' || v_rows_affected || ', Default rows: ' || v_default_row_count,
        'Success'
    );
 
EXCEPTION
    WHEN OTHERS THEN
        CALL BL_CL.LOG_PROCEDURE(
            'LOAD_CE_EMPLOYEES',
            -1,
            'Error: ' || SQLERRM,
            'Error'
        );
        RAISE;
	END;
END;
$$;
 
CALL BL_CL.LOAD_CE_EMPLOYEES();
 
SELECT * FROM BL_CL.LOG_TABLE 
WHERE procedure_name = 'LOAD_CE_EMPLOYEES' 
ORDER BY execution_time DESC;

SELECT * FROM BL_3NF.CE_EMPLOYEES;