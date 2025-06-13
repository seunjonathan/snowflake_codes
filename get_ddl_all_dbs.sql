
CREATE OR REPLACE PROCEDURE SAVE_ALL_DATABASE_DDLS()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    // Create results table if it doesn't exist
    var create_table = `CREATE TRANSIENT TABLE IF NOT EXISTS DATABASE_DDLS (
        DATABASE_NAME STRING,
        DDL STRING,
        TIMESTAMP TIMESTAMP_LTZ
    )`;
    snowflake.execute({sqlText: create_table});
    
    // Clear previous results
    snowflake.execute({sqlText: `TRUNCATE TABLE DATABASE_DDLS`});
    
    // Get all databases
    var db_query = `SELECT DATABASE_NAME FROM INFORMATION_SCHEMA.DATABASES WHERE DATABASE_NAME NOT IN ('INFORMATION_SCHEMA')`;
    var db_stmt = snowflake.createStatement({sqlText: db_query});
    var db_results = db_stmt.execute();
    
    var count = 0;
    
    // Loop through each database
    while (db_results.next()) {
        var db_name = db_results.getColumnValue(1);
        
        try {
            // Get DDL for the database
            var ddl_query = `SELECT GET_DDL('DATABASE', '${db_name}')`;
            var ddl_stmt = snowflake.createStatement({sqlText: ddl_query});
            var ddl_results = ddl_stmt.execute();
            
            if (ddl_results.next()) {
                var insert_query = `INSERT INTO DATABASE_DDLS VALUES (
                    '${db_name}',
                    '${ddl_results.getColumnValue(1).replace(/'/g, "''")}',
                    CURRENT_TIMESTAMP()
                )`;
                snowflake.execute({sqlText: insert_query});
                count++;
            }
        } catch (err) {
            snowflake.execute({
                sqlText: `INSERT INTO DATABASE_DDLS VALUES (
                    '${db_name}',
                    '-- Error getting DDL: ${err}',
                    CURRENT_TIMESTAMP()
                )`
            });
        }
    }
    
    return `Successfully captured DDL for ${count} databases`;
$$;

-- Execute the procedure
CALL SAVE_ALL_DATABASE_DDLS();

-- View the results
SELECT * FROM DATABASE_DDLS;
