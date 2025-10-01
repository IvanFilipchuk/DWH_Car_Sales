DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bl_cl') THEN
        CREATE ROLE BL_CL;
    END IF;
END $$;
 
CREATE SCHEMA IF NOT EXISTS BL_CL;
 
GRANT USAGE ON SCHEMA BL_3NF TO BL_CL;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA BL_3NF TO BL_CL;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA BL_3NF TO BL_CL;
 
GRANT USAGE ON SCHEMA BL_CL TO BL_CL;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA BL_CL TO BL_CL;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA BL_CL TO BL_CL;

GRANT USAGE ON SCHEMA BL_DM TO BL_CL;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA BL_DM TO BL_CL;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA BL_DM TO BL_CL;

 
CREATE TABLE IF NOT EXISTS BL_CL.LOG_TABLE (
    log_id SERIAL PRIMARY KEY,
    procedure_name VARCHAR(255) NOT NULL,
    execution_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    rows_affected INTEGER,
    log_message TEXT,
    status VARCHAR(50)
);
 
CREATE OR REPLACE PROCEDURE BL_CL.LOG_PROCEDURE(
    p_procedure_name VARCHAR,
    p_rows_affected INTEGER,
    p_log_message TEXT,
    p_status VARCHAR
)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO BL_CL.LOG_TABLE (
        procedure_name, 
        rows_affected, 
        log_message, 
        status
    )
    VALUES (
        p_procedure_name, 
        p_rows_affected, 
        p_log_message, 
        p_status
    );
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO BL_CL.LOG_TABLE (
            procedure_name, 
            rows_affected, 
            log_message, 
            status
        )
        VALUES (
            p_procedure_name, 
            -1, 
            'Logging failed: ' || SQLERRM, 
            'Error'
        );
END;
$$;