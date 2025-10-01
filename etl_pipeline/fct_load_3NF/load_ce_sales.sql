DROP TYPE IF EXISTS BL_CL.CE_SALES_TYPE CASCADE;
CREATE TYPE BL_CL.CE_SALES_TYPE AS (
    sale_src_id VARCHAR(50),
    source_id VARCHAR(100),
    customer_id INTEGER,
    employee_id INTEGER,
    car_id INTEGER,
    dealer_id INTEGER,
    date_id INTEGER,
    sales_channel_id INTEGER,
    payment_method_id INTEGER,
    sale_price DECIMAL(12,2),
    commission_rate DECIMAL(20,18),
    commission_earned DECIMAL(12,2),
    reduction_percent DECIMAL(5,2),
    income DECIMAL(12,2),
    source_system VARCHAR(50),
    source_entity VARCHAR(50)
);

CREATE OR REPLACE FUNCTION BL_CL.FN_GET_SALES_TO_LOAD()
RETURNS SETOF BL_CL.CE_SALES_TYPE
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT
        UPPER(TRIM(o.id))::VARCHAR(50) AS sale_src_id,
        ('OFFLINE_' || UPPER(TRIM(o.id)))::VARCHAR(100) AS source_id,
        COALESCE(cust.CUSTOMER_ID, -1) AS customer_id,  
        COALESCE(emp.EMPLOYEE_ID, -1) AS employee_id,   
        COALESCE(car.CAR_ID, -1) AS car_id,             
        COALESCE(d.DEALER_ID, -1) AS dealer_id,        
        COALESCE(dt.DATE_ID, -1) AS date_id,           
        COALESCE(sc.SALES_CHANNEL_ID, -1) AS sales_channel_id,  
        COALESCE(pm.PAYMENT_METHOD_ID, -1) AS payment_method_id,
        (CASE 
            WHEN UPPER(TRIM(o.saleprice)) ~ '^\d+(\.\d+)?$' 
            THEN UPPER(TRIM(o.saleprice))::DECIMAL(12,2)
            ELSE NULL::DECIMAL(12,2)
        END)::DECIMAL(12,2) AS sale_price,
        (CASE 
            WHEN UPPER(TRIM(o.commissionrate)) ~ '^\d+(\.\d+)?$' 
            THEN UPPER(TRIM(o.commissionrate))::DECIMAL(20,18)
            ELSE NULL::DECIMAL(20,18)
        END)::DECIMAL(20,18) AS commission_rate,
        (CASE 
            WHEN UPPER(TRIM(o.commissionearned)) ~ '^\d+(\.\d+)?$' 
            THEN UPPER(TRIM(o.commissionearned))::DECIMAL(12,2)
            ELSE NULL::DECIMAL(12,2)
        END)::DECIMAL(12,2) AS commission_earned,
        NULL::DECIMAL(5,2) AS reduction_percent,
        NULL::DECIMAL(12,2) AS income,
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
    LEFT JOIN BL_3NF.CE_CUSTOMERS_SDC cust ON cust.CUSTOMER_SRC_ID = CONCAT(
        UPPER(TRIM(COALESCE(o.customername, 'n. a.'))), '_',
        UPPER(TRIM(COALESCE(o.customeremail, 'n. a.'))), '_',
        COALESCE(a.ADDRESS_ID, -1)  
    )
        AND cust.IS_ACTIVE = 'Y'
        AND cust.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    LEFT JOIN BL_3NF.CE_EMPLOYEES emp ON emp.EMPLOYEE_SRC_ID = UPPER(TRIM(COALESCE(o.salespersonid::TEXT, 'n. a.')))
        AND emp.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    LEFT JOIN BL_3NF.CE_CAR_BRANDS brand ON brand.BRAND_SRC_ID = UPPER(TRIM(o.carmake))
        AND brand.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    LEFT JOIN BL_3NF.CE_CAR_MODELS model ON model.MODEL_SRC_ID = CONCAT(UPPER(TRIM(o.carmodel)), '_', COALESCE(brand.CAR_BRAND_ID, -1)) 
        AND model.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    LEFT JOIN BL_3NF.CE_CARS car ON car.CAR_SRC_ID = CONCAT(
        UPPER(TRIM(o.carmake)), '_', 
        UPPER(TRIM(o.carmodel)), '_', 
        UPPER(TRIM(COALESCE(o.caryear, 'n. a.')))
    )
        AND car.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    LEFT JOIN BL_3NF.CE_DEALERS d ON d.DEALER_SRC_ID = UPPER(TRIM(o.cardelername))
        AND d.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    LEFT JOIN BL_3NF.CE_DATES dt ON dt.DATE = (
        CASE 
            WHEN o.date ~ '^\d{4}-\d{2}-\d{2}$' OR o.date ~ '^\d{2}/\d{2}/\d{4}$'
            THEN o.date::DATE
            ELSE '1900-01-01'::DATE
        END
    )
    LEFT JOIN BL_3NF.CE_SALES_CHANNELS sc ON sc.SALES_CHANNEL_SRC_ID = UPPER(TRIM(o.saleschannel))
        AND sc.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    LEFT JOIN BL_3NF.CE_PAYMENT_METHODS pm ON pm.PAYMENT_METHOD_SRC_ID = UPPER(TRIM(o.paymentmethod))
        AND pm.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    WHERE o.id IS NOT NULL
        AND TRIM(o.id) != ''
        AND NOT EXISTS (
            SELECT 1 FROM BL_3NF.CE_SALES s
            WHERE s.SALE_SRC_ID = UPPER(TRIM(o.id))
            AND s.SOURCE_ID = 'OFFLINE_' || UPPER(TRIM(o.id))
        )

    UNION ALL

    SELECT DISTINCT
        UPPER(TRIM(o.id))::VARCHAR(50) AS sale_src_id,
        ('ONLINE_' || UPPER(TRIM(o.id)))::VARCHAR(100) AS source_id,
        COALESCE(cust.CUSTOMER_ID, -1) AS customer_id,  
        COALESCE(emp.EMPLOYEE_ID, -1) AS employee_id,  
        COALESCE(car.CAR_ID, -1) AS car_id,            
        COALESCE(d.DEALER_ID, -1) AS dealer_id,        
        COALESCE(dt.DATE_ID, -1) AS date_id,            
        COALESCE(sc.SALES_CHANNEL_ID, -1) AS sales_channel_id,  
        COALESCE(pm.PAYMENT_METHOD_ID, -1) AS payment_method_id, 
        (CASE 
            WHEN UPPER(TRIM(o.car_price)) ~ '^\d+(\.\d+)?$' 
            THEN UPPER(TRIM(o.car_price))::DECIMAL(12,2)
            ELSE NULL::DECIMAL(12,2)
        END)::DECIMAL(12,2) AS sale_price,
        NULL::DECIMAL(20,18) AS commission_rate,
        NULL::DECIMAL(12,2) AS commission_earned,
        (CASE 
            WHEN UPPER(TRIM(o.reduction_percent)) ~ '^\d+(\.\d+)?$' 
            THEN UPPER(TRIM(o.reduction_percent))::DECIMAL(5,2)
            ELSE NULL::DECIMAL(5,2)
        END)::DECIMAL(5,2) AS reduction_percent,
        (CASE 
            WHEN UPPER(TRIM(o.income)) ~ '^\d+(\.\d+)?$' 
            THEN UPPER(TRIM(o.income))::DECIMAL(12,2)
            ELSE NULL::DECIMAL(12,2)
        END)::DECIMAL(12,2) AS income,
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
    LEFT JOIN BL_3NF.CE_CUSTOMERS_SDC cust ON cust.CUSTOMER_SRC_ID = CONCAT(
        UPPER(TRIM(COALESCE(o.customer_name_and_surname, 'n. a.'))), '_',
        UPPER(TRIM(COALESCE(o.customer_email, 'n. a.'))), '_',
        COALESCE(a.ADDRESS_ID, -1) 
    )
        AND cust.IS_ACTIVE = 'Y'
        AND cust.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    LEFT JOIN BL_3NF.CE_EMPLOYEES emp ON emp.EMPLOYEE_SRC_ID = UPPER(TRIM(COALESCE(o.employee_id::TEXT, 'n. a.')))
        AND emp.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    LEFT JOIN BL_3NF.CE_CAR_BRANDS brand ON brand.BRAND_SRC_ID = UPPER(TRIM(o.brand))
        AND brand.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    LEFT JOIN BL_3NF.CE_CAR_MODELS model ON model.MODEL_SRC_ID = CONCAT(UPPER(TRIM(o.car_model)), '_', COALESCE(brand.CAR_BRAND_ID, -1)) 
        AND model.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    LEFT JOIN BL_3NF.CE_CARS car ON car.CAR_SRC_ID = CONCAT(
        UPPER(TRIM(o.brand)), '_', 
        UPPER(TRIM(o.car_model)), '_', 
        UPPER(TRIM(COALESCE(o.car_year, 'n. a.')))
    )
        AND car.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    LEFT JOIN BL_3NF.CE_DEALERS d ON d.DEALER_SRC_ID = UPPER(TRIM(o.car_dealer_name))
        AND d.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    LEFT JOIN BL_3NF.CE_DATES dt ON dt.DATE = (
        CASE 
            WHEN o.date_of_transaction ~ '^\d{4}-\d{2}-\d{2}$' OR o.date_of_transaction ~ '^\d{2}/\d{2}/\d{4}$'
            THEN o.date_of_transaction::DATE
            ELSE '1900-01-01'::DATE
        END
    )
    LEFT JOIN BL_3NF.CE_SALES_CHANNELS sc ON sc.SALES_CHANNEL_SRC_ID = UPPER(TRIM(o.sales_channel))
        AND sc.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    LEFT JOIN BL_3NF.CE_PAYMENT_METHODS pm ON pm.PAYMENT_METHOD_SRC_ID = UPPER(TRIM(o.payment_method))
        AND pm.SOURCE_SYSTEM IN ('SA_CAR_SALES_OFFLINE', 'SA_CAR_SALES_ONLINE')
    WHERE o.id IS NOT NULL
        AND TRIM(o.id) != ''
        AND NOT EXISTS (
            SELECT 1 FROM BL_3NF.CE_SALES s
            WHERE s.SALE_SRC_ID = UPPER(TRIM(o.id))
            AND s.SOURCE_ID = 'ONLINE_' || UPPER(TRIM(o.id))
        )
        AND NOT EXISTS (
            SELECT 1 FROM sa_car_sales_offline.src_car_sales_offline o2
            WHERE UPPER(TRIM(o2.id)) = UPPER(TRIM(o.id))
        );
END;
$$;

CREATE OR REPLACE PROCEDURE BL_CL.LOAD_CE_SALES()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_inserted INTEGER := 0;
    sale_rec BL_CL.CE_SALES_TYPE;
BEGIN
	BEGIN
    RAISE NOTICE 'Starting LOAD_CE_SALES...';
    
    FOR sale_rec IN 
        SELECT 
            sale_data.sale_src_id,
            sale_data.source_id,
            sale_data.customer_id,
            sale_data.employee_id,
            sale_data.car_id,
            sale_data.dealer_id,
            sale_data.date_id,
            sale_data.sales_channel_id,
            sale_data.payment_method_id,
            sale_data.sale_price,
            sale_data.commission_rate,
            sale_data.commission_earned,
            sale_data.reduction_percent,
            sale_data.income,
            sale_data.source_system,
            sale_data.source_entity
        FROM BL_CL.FN_GET_SALES_TO_LOAD() sale_data
        WHERE NOT EXISTS (
            SELECT 1 FROM BL_3NF.CE_SALES s
            WHERE s.SALE_SRC_ID = sale_data.sale_src_id
            AND s.SOURCE_ID = sale_data.source_id
        )
    LOOP
        INSERT INTO BL_3NF.CE_SALES (
            SALE_ID, SALE_SRC_ID, SOURCE_ID, CUSTOMER_ID, EMPLOYEE_ID, CAR_ID, 
            DEALER_ID, DATE_ID, SALES_CHANNEL_ID, PAYMENT_METHOD_ID, SALE_PRICE, 
            COMMISSION_RATE, COMMISSION_EARNED, REDUCTION_PERCENT, INCOME,
            SOURCE_SYSTEM, SOURCE_ENTITY, INSERT_DT
        )
        VALUES (
            NEXTVAL('bl_3nf.ce_sales_seq'),
            sale_rec.sale_src_id,
            sale_rec.source_id,
            sale_rec.customer_id,
            sale_rec.employee_id,
            sale_rec.car_id,
            sale_rec.dealer_id,
            sale_rec.date_id,
            sale_rec.sales_channel_id,
            sale_rec.payment_method_id,
            sale_rec.sale_price,
            sale_rec.commission_rate,
            sale_rec.commission_earned,
            sale_rec.reduction_percent,
            sale_rec.income,
            sale_rec.source_system,
            sale_rec.source_entity,
            CURRENT_DATE
        );
        
        v_rows_inserted := v_rows_inserted + 1;
    END LOOP;
    
    RAISE NOTICE 'Finished processing sales. Inserted: %', v_rows_inserted;

    CALL BL_CL.LOG_PROCEDURE(
        'LOAD_CE_SALES',
        v_rows_inserted,
        'Successfully loaded sales. Inserted: ' || v_rows_inserted,
        'Success'
    );
    
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error in LOAD_CE_SALES: %', SQLERRM;
        CALL BL_CL.LOG_PROCEDURE(
            'LOAD_CE_SALES',
            -1,
            'Error: ' || SQLERRM,
            'Error'
        );
        RAISE;
	END;
END;
$$;

CALL BL_CL.LOAD_CE_SALES();

SELECT * FROM BL_CL.LOG_TABLE
WHERE procedure_name = 'LOAD_CE_SALES'
ORDER BY execution_time DESC;

SELECT * FROM BL_3NF.CE_SALES;