
USE ROLE CDE_ROLE;
USE WAREHOUSE CDE_WH;
USE SCHEMA CANLIN.HARMONIZED;


-- ----------------------------------------------------------------------------
-- Step #1: Create the tasks to call our Python stored procedures
-- ----------------------------------------------------------------------------

ALTER TASK AVOCET_PROD_DETAIL_UPDATE_TASK SUSPEND;
ALTER TASK AVOCET_WELL_STATS_TASK SUSPEND;
ALTER TASK LEASE_OPS_UPDATE_TASK SUSPEND;
-- ALTER TASK WELLS_HISTORIAN_RESAMPLE_TASK SUSPEND;

-- CRON Schedule every morning at 8am MST (UTC-7)

CREATE OR REPLACE TASK AVOCET_PROD_DETAIL_UPDATE_TASK
  WAREHOUSE = CDE_WH
  -- SCHEDULE = '1 minute'
  SCHEDULE = 'USING CRON 0 8,20 * * * America/Edmonton'
WHEN SYSTEM$STREAM_HAS_DATA('RAW_AVOCET.AVOCET_WELL_PROD_STREAM')
AS 
  CALL HARMONIZED.AVOCET_PROD_DETAIL_SP();

CREATE OR REPLACE TASK AVOCET_WELL_STATS_TASK
WAREHOUSE = CDE_WH
  AFTER AVOCET_PROD_DETAIL_UPDATE_TASK
AS
CALL HARMONIZED.AVOCET_WELL_STATS_SP();

CREATE OR REPLACE TASK WEEKLY_PRODUCTION_REPORT_UPDATE_TASK
  WAREHOUSE = CDE_WH
  -- SCHEDULE = every 30 minutes
  SCHEDULE = '30 MINUTE'
AS 
  CALL HARMONIZED.WEEKLY_PRODUCTION_REPORT_SP();


-- CREATE OR REPLACE TASK TRAIN_SET_WELLS_OPERATIONS_TASK
-- WAREHOUSE = CDE_WH
--   AFTER AVOCET_PROD_DETAIL_UPDATE_TASK
-- AS
-- CALL HARMONIZED.TRAIN_SET_GAS_WELLS_OPERATIONS_SP();


-- ----------------------------------------------------------------------------
-- Step #2: Execute the tasks
-- ----------------------------------------------------------------------------

-- Resume from top level down
-- ALTER TASK TRAIN_SET_WELLS_OPERATIONS_TASK RESUME;
-- ALTER TASK TRAIN_SET_WELLS_OPERATIONS_TASK RESUME;
ALTER TASK AVOCET_PROD_DETAIL_UPDATE_TASK RESUME;
ALTER TASK AVOCET_WELL_STATS_TASK RESUME;
ALTER TASK LEASE_OPS_UPDATE_TASK RESUME;
-- EXECUTE TASK LEASE_OPS_UPDATE_TASK;

-- ----------------------------------------------------------------------------
-- Step #3: Monitor tasks in Snowsight
-- ----------------------------------------------------------------------------

/*---
-- TODO: Add Snowsight details here
-- https://docs.snowflake.com/en/user-guide/ui-snowsight-tasks.html



-- Alternatively, here are some manual queries to get at the same details
SHOW TASKS;

-- Task execution history in the past day
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START=>DATEADD('DAY',-1,CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 100))
ORDER BY SCHEDULED_TIME DESC
;

-- Scheduled task runs
SELECT
    TIMESTAMPDIFF(SECOND, CURRENT_TIMESTAMP, SCHEDULED_TIME) NEXT_RUN,
    SCHEDULED_TIME,
    NAME,
    STATE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE STATE = 'SCHEDULED'
ORDER BY COMPLETED_TIME DESC;

-- Other task-related metadata queries
SELECT *
  FROM TABLE(INFORMATION_SCHEMA.CURRENT_TASK_GRAPHS())
  ORDER BY SCHEDULED_TIME;

SELECT *
  FROM TABLE(INFORMATION_SCHEMA.COMPLETE_TASK_GRAPHS())
  ORDER BY SCHEDULED_TIME;
---*/