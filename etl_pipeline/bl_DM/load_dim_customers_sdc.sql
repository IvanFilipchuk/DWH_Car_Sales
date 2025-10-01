CREATE INDEX IF NOT EXISTS IDX_DIM_CUSTOMERS_SRC_ACTIVE
ON BL_DM.DIM_CUSTOMERS_SDC (CUSTOMER_SRC_ID, IS_ACTIVE);

DROP TYPE IF EXISTS BL_CL.DIM_CUSTOMERS_SDC_TYPE CASCADE;

CREATE TYPE BL_CL.DIM_CUSTOMERS_SDC_TYPE AS (
    CUSTOMER_ID INTEGER,
    CUSTOMER_SRC_ID VARCHAR(300),
    CUSTOMER_FIRST_NAME VARCHAR(100),
    CUSTOMER_LAST_NAME VARCHAR(100),
    CUSTOMER_EMAIL VARCHAR(100),
    CUSTOMER_TELEPHONE_NUMBER VARCHAR(30),
    CUSTOMER_COUNTRY VARCHAR(100),
    CUSTOMER_STATE VARCHAR(100),
    CUSTOMER_CITY VARCHAR(100),
    CUSTOMER_STREET_NAME VARCHAR(100),
    CUSTOMER_BUILDING_NUMBER VARCHAR(20),
    CUSTOMER_POSTAL_CODE VARCHAR(20),
    START_DT DATE,
    END_DT DATE,
    IS_ACTIVE CHAR(1),
    SOURCE_SYSTEM VARCHAR(50),
    SOURCE_ENTITY VARCHAR(50)
);

CREATE OR REPLACE FUNCTION BL_CL.GET_DIM_CUSTOMERS_SDC_DATA()
RETURNS SETOF BL_CL.DIM_CUSTOMERS_SDC_TYPE
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        c.CUSTOMER_ID,
        c.CUSTOMER_ID::VARCHAR(300) as CUSTOMER_SRC_ID,
        c.CUSTOMER_FIRST_NAME,
        c.CUSTOMER_LAST_NAME,
        c.CUSTOMER_EMAIL,
        c.CUSTOMER_TELEPHONE_NUMBER,
        co.COUNTRY_NAME::VARCHAR(100) as CUSTOMER_COUNTRY,
        s.STATE_NAME::VARCHAR(100) as CUSTOMER_STATE,
        ci.CITY_NAME::VARCHAR(100) as CUSTOMER_CITY,
        st.STREET_NAME::VARCHAR(100) as CUSTOMER_STREET_NAME,
        a.BUILDING_NUMBER::VARCHAR(20) as CUSTOMER_BUILDING_NUMBER,
        a.POSTAL_CODE::VARCHAR(20) as CUSTOMER_POSTAL_CODE,
        c.START_DT,         
        c.END_DT,             
        c.IS_ACTIVE::CHAR(1), 
        'BL_3NF'::VARCHAR(50) AS SOURCE_SYSTEM,
        'CE_CUSTOMERS_SDC'::VARCHAR(50) AS SOURCE_ENTITY
    FROM BL_3NF.CE_CUSTOMERS_SDC c
    LEFT JOIN BL_3NF.CE_ADDRESSES a ON c.ADDRESS_ID = a.ADDRESS_ID
    LEFT JOIN BL_3NF.CE_STREETS st ON a.STREET_ID = st.STREET_ID
    LEFT JOIN BL_3NF.CE_CITIES ci ON st.CITY_ID = ci.CITY_ID
    LEFT JOIN BL_3NF.CE_STATES s ON ci.STATE_ID = s.STATE_ID
    LEFT JOIN BL_3NF.CE_COUNTRIES co ON s.COUNTRY_ID = co.COUNTRY_ID
    WHERE c.CUSTOMER_ID > 0;
END;
$$;


CREATE OR REPLACE PROCEDURE BL_CL.LOAD_DIM_CUSTOMERS_SDC()
LANGUAGE plpgsql
AS $$
DECLARE
    cur_customers REFCURSOR;
    v_customer_record BL_CL.DIM_CUSTOMERS_SDC_TYPE;
    v_sql TEXT;
    v_rows_inserted INTEGER := 0;
    v_rows_closed INTEGER := 0;
    v_default_row_count INTEGER := 0;
    v_exists_count INTEGER;
    v_record_changed BOOLEAN;
    v_counter INTEGER := 0;
BEGIN
	BEGIN
    RAISE NOTICE 'Starting DIM_CUSTOMERS_SDC load...';
    
    INSERT INTO BL_DM.DIM_CUSTOMERS_SDC (
        CUSTOMER_SURR_ID, CUSTOMER_SRC_ID, CUSTOMER_FIRST_NAME, CUSTOMER_LAST_NAME,
        CUSTOMER_EMAIL, CUSTOMER_TELEPHONE_NUMBER, CUSTOMER_COUNTRY, CUSTOMER_STATE,
        CUSTOMER_CITY, CUSTOMER_STREET_NAME, CUSTOMER_BUILDING_NUMBER, CUSTOMER_POSTAL_CODE,
        START_DT, END_DT, IS_ACTIVE, INSERT_DT, SOURCE_SYSTEM, SOURCE_ENTITY
    )
    SELECT -1, '-1', 'n. a.', 'n. a.', 'n. a.', 'n. a.', 'n. a.', 'n. a.',
           'n. a.', 'n. a.', 'n. a.', 'n. a.', CURRENT_DATE, NULL, 'Y', 
           CURRENT_DATE, 'MANUAL', 'DEFAULT_ROW'
    WHERE NOT EXISTS (
        SELECT 1 FROM BL_DM.DIM_CUSTOMERS_SDC WHERE CUSTOMER_SURR_ID = -1
    );

    GET DIAGNOSTICS v_default_row_count = ROW_COUNT;
    RAISE NOTICE 'Default row count: %', v_default_row_count;

    v_sql := 'SELECT * FROM BL_CL.GET_DIM_CUSTOMERS_SDC_DATA()';
    
    OPEN cur_customers FOR EXECUTE v_sql;
    
    LOOP
        FETCH cur_customers INTO v_customer_record;
        EXIT WHEN NOT FOUND;
        
        v_counter := v_counter + 1;
        
        IF v_counter % 1000 = 0 THEN
            RAISE NOTICE 'Processing record %: Customer ID %', v_counter, v_customer_record.CUSTOMER_SRC_ID;
        END IF;
        
        SELECT COUNT(*) INTO v_exists_count 
        FROM BL_DM.DIM_CUSTOMERS_SDC 
        WHERE CUSTOMER_SRC_ID = v_customer_record.CUSTOMER_SRC_ID
        AND CUSTOMER_FIRST_NAME = v_customer_record.CUSTOMER_FIRST_NAME
        AND CUSTOMER_LAST_NAME = v_customer_record.CUSTOMER_LAST_NAME
        AND CUSTOMER_EMAIL = v_customer_record.CUSTOMER_EMAIL
        AND CUSTOMER_TELEPHONE_NUMBER = v_customer_record.CUSTOMER_TELEPHONE_NUMBER
        AND CUSTOMER_COUNTRY = v_customer_record.CUSTOMER_COUNTRY
        AND CUSTOMER_STATE = v_customer_record.CUSTOMER_STATE
        AND CUSTOMER_CITY = v_customer_record.CUSTOMER_CITY
        AND CUSTOMER_STREET_NAME = v_customer_record.CUSTOMER_STREET_NAME
        AND CUSTOMER_BUILDING_NUMBER = v_customer_record.CUSTOMER_BUILDING_NUMBER
        AND CUSTOMER_POSTAL_CODE = v_customer_record.CUSTOMER_POSTAL_CODE
        AND START_DT = v_customer_record.START_DT
        AND COALESCE(END_DT, '9999-12-31') = COALESCE(v_customer_record.END_DT, '9999-12-31')
        AND IS_ACTIVE = v_customer_record.IS_ACTIVE;
        
        IF v_exists_count = 0 THEN
            
            SELECT COUNT(*) INTO v_exists_count 
            FROM BL_DM.DIM_CUSTOMERS_SDC 
            WHERE CUSTOMER_SRC_ID = v_customer_record.CUSTOMER_SRC_ID
            AND IS_ACTIVE = 'Y';
            
            IF v_exists_count > 0 THEN
                SELECT EXISTS (
                    SELECT 1 FROM BL_DM.DIM_CUSTOMERS_SDC d
                    JOIN (
                        SELECT 
                            v_customer_record.CUSTOMER_FIRST_NAME as first_name,
                            v_customer_record.CUSTOMER_LAST_NAME as last_name,
                            v_customer_record.CUSTOMER_EMAIL as email,
                            v_customer_record.CUSTOMER_TELEPHONE_NUMBER as phone,
                            v_customer_record.CUSTOMER_COUNTRY as country,
                            v_customer_record.CUSTOMER_STATE as state,
                            v_customer_record.CUSTOMER_CITY as city,
                            v_customer_record.CUSTOMER_STREET_NAME as street,
                            v_customer_record.CUSTOMER_BUILDING_NUMBER as building,
                            v_customer_record.CUSTOMER_POSTAL_CODE as postal
                    ) new ON (
                        d.CUSTOMER_FIRST_NAME IS DISTINCT FROM new.first_name OR
                        d.CUSTOMER_LAST_NAME IS DISTINCT FROM new.last_name OR
                        d.CUSTOMER_EMAIL IS DISTINCT FROM new.email OR
                        d.CUSTOMER_TELEPHONE_NUMBER IS DISTINCT FROM new.phone OR
                        d.CUSTOMER_COUNTRY IS DISTINCT FROM new.country OR
                        d.CUSTOMER_STATE IS DISTINCT FROM new.state OR
                        d.CUSTOMER_CITY IS DISTINCT FROM new.city OR
                        d.CUSTOMER_STREET_NAME IS DISTINCT FROM new.street OR
                        d.CUSTOMER_BUILDING_NUMBER IS DISTINCT FROM new.building OR
                        d.CUSTOMER_POSTAL_CODE IS DISTINCT FROM new.postal
                    )
                    WHERE d.CUSTOMER_SRC_ID = v_customer_record.CUSTOMER_SRC_ID
                    AND d.IS_ACTIVE = 'Y'
                ) INTO v_record_changed;
                
                IF v_record_changed THEN
                    UPDATE BL_DM.DIM_CUSTOMERS_SDC 
                    SET 
                        END_DT = CURRENT_DATE - 1,
                        IS_ACTIVE = 'N'::CHAR(1),
                        UPDATE_DT = CURRENT_DATE
                    WHERE CUSTOMER_SRC_ID = v_customer_record.CUSTOMER_SRC_ID
                    AND IS_ACTIVE = 'Y';
                    
                    GET DIAGNOSTICS v_rows_closed = ROW_COUNT;
                END IF;
            END IF;
            
            INSERT INTO BL_DM.DIM_CUSTOMERS_SDC (
                CUSTOMER_SURR_ID, 
                CUSTOMER_SRC_ID,
                CUSTOMER_FIRST_NAME,
                CUSTOMER_LAST_NAME,
                CUSTOMER_EMAIL,
                CUSTOMER_TELEPHONE_NUMBER,
                CUSTOMER_COUNTRY,
                CUSTOMER_STATE,
                CUSTOMER_CITY,
                CUSTOMER_STREET_NAME,
                CUSTOMER_BUILDING_NUMBER,
                CUSTOMER_POSTAL_CODE,
                START_DT,
                END_DT,
                IS_ACTIVE,
                INSERT_DT, 
                SOURCE_SYSTEM, 
                SOURCE_ENTITY
            )
            VALUES (
                NEXTVAL('bl_dm.dim_customers_sdc_seq'),
                v_customer_record.CUSTOMER_SRC_ID,
                v_customer_record.CUSTOMER_FIRST_NAME,
                v_customer_record.CUSTOMER_LAST_NAME,
                v_customer_record.CUSTOMER_EMAIL,
                v_customer_record.CUSTOMER_TELEPHONE_NUMBER,
                v_customer_record.CUSTOMER_COUNTRY,
                v_customer_record.CUSTOMER_STATE,
                v_customer_record.CUSTOMER_CITY,
                v_customer_record.CUSTOMER_STREET_NAME,
                v_customer_record.CUSTOMER_BUILDING_NUMBER,
                v_customer_record.CUSTOMER_POSTAL_CODE,
                v_customer_record.START_DT,
                v_customer_record.END_DT,
                v_customer_record.IS_ACTIVE,
                CURRENT_DATE,
                v_customer_record.SOURCE_SYSTEM,
                v_customer_record.SOURCE_ENTITY
            );
            v_rows_inserted := v_rows_inserted + 1;
        END IF;
    END LOOP;
    
    CLOSE cur_customers;

    RAISE NOTICE 'Finished processing % records. Inserted: %, Closed: %', v_counter, v_rows_inserted, v_rows_closed;

    CALL BL_CL.LOG_PROCEDURE(
        'LOAD_DIM_CUSTOMERS_SDC',
        v_rows_inserted + v_rows_closed + v_default_row_count,
        'Successfully loaded DIM_CUSTOMERS_SDC. Processed: ' || v_counter || ', Inserted: ' || v_rows_inserted || ', Closed: ' || v_rows_closed || ', Default rows: ' || v_default_row_count,
        'Success'
    );

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error in LOAD_DIM_CUSTOMERS_SDC: %', SQLERRM;
        CALL BL_CL.LOG_PROCEDURE(
            'LOAD_DIM_CUSTOMERS_SDC',
            -1,
            'Error: ' || SQLERRM,
            'Error'
        );
        RAISE;
	END;
END;
$$;

CALL BL_CL.LOAD_DIM_CUSTOMERS_SDC();

SELECT * FROM BL_CL.LOG_TABLE 
WHERE procedure_name = 'LOAD_DIM_CUSTOMERS_SDC' 
ORDER BY execution_time DESC;

SELECT * FROM BL_DM.DIM_CUSTOMERS_SDC;