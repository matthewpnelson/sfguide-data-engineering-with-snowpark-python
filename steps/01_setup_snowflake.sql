
-- ----------------------------------------------------------------------------
-- Step #1: Accept Anaconda Terms & Conditions
-- ----------------------------------------------------------------------------

-- See Getting Started section in Third-Party Packages (https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-packages.html#getting-started)


-- ----------------------------------------------------------------------------
-- Step #2: Create the account level objects
-- ----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- Roles
SET MY_USER = CURRENT_USER();
CREATE OR REPLACE ROLE HOL_ROLE;
GRANT ROLE HOL_ROLE TO ROLE SYSADMIN;
GRANT ROLE HOL_ROLE TO USER IDENTIFIER($MY_USER);

GRANT EXECUTE TASK ON ACCOUNT TO ROLE HOL_ROLE;
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE HOL_ROLE;
-- GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE HOL_ROLE;

-- Databases
-- CREATE OR REPLACE DATABASE CDE_DB;
GRANT OWNERSHIP ON DATABASE HOL_DB TO ROLE HOL_ROLE;

-- Warehouses
CREATE OR REPLACE WAREHOUSE HOL_WH WAREHOUSE_SIZE = XSMALL, AUTO_SUSPEND = 300, AUTO_RESUME= TRUE;
GRANT OWNERSHIP ON WAREHOUSE HOL_WH TO ROLE HOL_ROLE;


-- ----------------------------------------------------------------------------
-- Step #3: Create the database level objects
-- ----------------------------------------------------------------------------
USE ROLE HOL_ROLE;
USE WAREHOUSE HOL_WH;
USE DATABASE HOL_DB;

-- Schemas
-- CREATE OR REPLACE SCHEMA EXTERNAL;
-- CREATE OR REPLACE SCHEMA RAW_POS;
-- CREATE OR REPLACE SCHEMA RAW_CUSTOMER;
CREATE OR REPLACE SCHEMA HARMONIZED;
CREATE OR REPLACE SCHEMA ANALYTICS;

-- External Frostbyte objects
-- USE SCHEMA EXTERNAL;
-- CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT
--     TYPE = PARQUET
--     COMPRESSION = SNAPPY
-- ;
-- CREATE OR REPLACE STAGE FROSTBYTE_RAW_STAGE
--     URL = 's3://sfquickstarts/data-engineering-with-snowpark-python/'
-- ;

-- Unit Conversion Functions
USE SCHEMA HARMONIZED;
CREATE OR REPLACE FUNCTION HARMONIZED.BBL_TO_M3_UDF(BBL NUMBER(35,4))
RETURNS NUMBER(35,4)
AS
$$
   bbl / 6.29287
$$;

CREATE OR REPLACE FUNCTION HARMONIZED.M3_TO_BBL_UDF(M3 NUMBER(35,4))
RETURNS NUMBER(35,4)
    AS
$$
    m3 * 6.29287
$$;

CREATE OR REPLACE FUNCTION HARMONIZED.MCF_TO_E3M3_UDF(MCF NUMBER(35,4))
RETURNS NUMBER(35,4)
    AS
$$
    mcf / 35.49373
$$;
CREATE OR REPLACE FUNCTION HARMONIZED.E3M3_TO_MCF_UDF(E3M3 NUMBER(35,4))
RETURNS NUMBER(35,4)
    AS
$$
    e3m3 * 35.49373
$$;
CREATE OR REPLACE FUNCTION HARMONIZED.PSI_TO_KPA_UDF(PSI NUMBER(35,4))
RETURNS NUMBER(35,4)
    AS
$$
    psi / 0.14503774
$$;
CREATE OR REPLACE FUNCTION HARMONIZED.KPA_TO_PSI_UDF(KPA NUMBER(35,4))
RETURNS NUMBER(35,4)
    AS
$$
    kpa * 0.14503774
$$;
CREATE OR REPLACE FUNCTION HARMONIZED.E3M3_TO_BOE_UDF(E3M3 NUMBER(35,4))
RETURNS NUMBER(35,4)
    AS
$$
    e3m3 * 5.915621600 
$$;
