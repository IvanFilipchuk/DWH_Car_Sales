DROP TYPE IF EXISTS BL_CL.DIM_CARS_TYPE CASCADE;
CREATE TYPE BL_CL.DIM_CARS_TYPE AS (
    CAR_ID INTEGER,
    CAR_SRC_ID VARCHAR(200),
    BRAND_NAME VARCHAR(100),
    MODEL_NAME VARCHAR(100),
    YEAR INTEGER,
    SOURCE_SYSTEM VARCHAR(50),
    SOURCE_ENTITY VARCHAR(50)
);

CREATE OR REPLACE FUNCTION BL_CL.GET_DIM_CARS_DATA()
RETURNS SETOF BL_CL.DIM_CARS_TYPE
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        c.CAR_ID,
        c.CAR_SRC_ID,
        b.BRAND_NAME,
        m.MODEL_NAME,
        c.YEAR,
        'BL_3NF'::VARCHAR(50) AS SOURCE_SYSTEM,
        'CE_CARS'::VARCHAR(50) AS SOURCE_ENTITY
    FROM BL_3NF.CE_CARS c
    JOIN BL_3NF.CE_CAR_MODELS m ON c.CAR_MODEL_ID = m.CAR_MODEL_ID
    JOIN BL_3NF.CE_CAR_BRANDS b ON m.CAR_BRAND_ID = b.CAR_BRAND_ID
    WHERE c.CAR_ID > 0;
END;
$$;


CREATE OR REPLACE PROCEDURE BL_CL.LOAD_DIM_CARS()
LANGUAGE plpgsql
AS $$
DECLARE
    cur_cars REFCURSOR;
    v_car_record BL_CL.DIM_CARS_TYPE;
    v_sql TEXT;
    v_rows_inserted INTEGER := 0;
    v_rows_updated INTEGER := 0;
    v_default_row_count INTEGER := 0;
    v_exists_count INTEGER;
BEGIN
	BEGIN
    INSERT INTO BL_DM.DIM_CARS (
        CAR_SURR_ID, CAR_SRC_ID, BRAND_NAME, MODEL_NAME, YEAR, 
        SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT
    )
    SELECT -1, '-1', 'n. a.', 'n. a.', -1, 'MANUAL', 'DEFAULT_ROW', CURRENT_DATE
    WHERE NOT EXISTS (
        SELECT 1 FROM BL_DM.DIM_CARS WHERE CAR_SURR_ID = -1
    );

    GET DIAGNOSTICS v_default_row_count = ROW_COUNT;

    v_sql := 'SELECT * FROM BL_CL.GET_DIM_CARS_DATA()';
    
    OPEN cur_cars FOR EXECUTE v_sql;
    
    LOOP
        FETCH cur_cars INTO v_car_record;
        EXIT WHEN NOT FOUND;
        
        SELECT COUNT(*) INTO v_exists_count 
        FROM BL_DM.DIM_CARS 
        WHERE CAR_SRC_ID = v_car_record.CAR_ID::TEXT;
        
        IF v_exists_count = 0 THEN
          
            INSERT INTO BL_DM.DIM_CARS (
                CAR_SURR_ID, CAR_SRC_ID, BRAND_NAME, MODEL_NAME, YEAR, 
                INSERT_DT, SOURCE_SYSTEM, SOURCE_ENTITY
            )
            VALUES (
                NEXTVAL('bl_dm.dm_cars_seq'), 
                v_car_record.CAR_ID::TEXT, 
                v_car_record.BRAND_NAME, 
                v_car_record.MODEL_NAME, 
                v_car_record.YEAR,
                CURRENT_DATE,
                v_car_record.SOURCE_SYSTEM,
                v_car_record.SOURCE_ENTITY
            );
            v_rows_inserted := v_rows_inserted + 1;
        ELSE
            UPDATE BL_DM.DIM_CARS 
            SET 
                BRAND_NAME = v_car_record.BRAND_NAME,
                MODEL_NAME = v_car_record.MODEL_NAME,
                YEAR = v_car_record.YEAR,
                SOURCE_SYSTEM = v_car_record.SOURCE_SYSTEM,
                SOURCE_ENTITY = v_car_record.SOURCE_ENTITY,
                UPDATE_DT = CURRENT_DATE
            WHERE CAR_SRC_ID = v_car_record.CAR_ID::TEXT
            AND (
                BRAND_NAME IS DISTINCT FROM v_car_record.BRAND_NAME OR
                MODEL_NAME IS DISTINCT FROM v_car_record.MODEL_NAME OR
                YEAR IS DISTINCT FROM v_car_record.YEAR OR
                SOURCE_SYSTEM IS DISTINCT FROM v_car_record.SOURCE_SYSTEM OR
                SOURCE_ENTITY IS DISTINCT FROM v_car_record.SOURCE_ENTITY
            );
            
            GET DIAGNOSTICS v_rows_updated = ROW_COUNT;
        END IF;
    END LOOP;
    
    CLOSE cur_cars;

    CALL BL_CL.LOG_PROCEDURE(
        'LOAD_DIM_CARS',
        v_rows_inserted + v_rows_updated + v_default_row_count,
        'Successfully loaded DIM_CARS. Inserted: ' || v_rows_inserted || ', Updated: ' || v_rows_updated || ', Default rows: ' || v_default_row_count,
        'Success'
    );

EXCEPTION
    WHEN OTHERS THEN
        CALL BL_CL.LOG_PROCEDURE(
            'LOAD_DIM_CARS',
            -1,
            'Error: ' || SQLERRM,
            'Error'
        );
        RAISE;
	END;
END;
$$;

CALL BL_CL.LOAD_DIM_CARS();

SELECT * FROM BL_CL.LOG_TABLE 
WHERE procedure_name = 'LOAD_DIM_CARS' 
ORDER BY execution_time DESC;

SELECT * FROM BL_DM.DIM_CARS;