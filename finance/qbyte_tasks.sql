
USE ROLE CDE_ROLE;
USE WAREHOUSE CDE_WH;
USE SCHEMA CANLIN.HARMONIZED;


-- ----------------------------------------------------------------------------
-- Step #1: Create the tasks to call our Python stored procedures
-- ----------------------------------------------------------------------------

ALTER TASK LEASE_OPS_UPDATE_TASK SUSPEND;
ALTER TASK SIMPLE_FINANCIAL_METRICS_TASK SUSPEND;

-- CRON to run on the 11th of each month at 2am UTC
 -- 0 2 10 * * UTC

CREATE OR REPLACE TASK LEASE_OPS_UPDATE_TASK
WAREHOUSE = CDE_WH
  SCHEDULE = 'USING CRON 0 2 11 * * UTC'
-- WHEN
--   SYSTEM$STREAM_HAS_DATA('POS_FLATTENED_V_STREAM')
AS
CALL HARMONIZED.LEASE_OPS_UPDATE_SP();

CREATE OR REPLACE TASK SIMPLE_FINANCIAL_METRICS_TASK
WAREHOUSE = CDE_WH
  AFTER LEASE_OPS_UPDATE_TASK
AS
CALL HARMONIZED.SIMPLE_FINANCIAL_METRICS_SP();



-- ----------------------------------------------------------------------------
-- Step #2: Execute the tasks
-- ----------------------------------------------------------------------------

-- Resume from top level down
ALTER TASK SIMPLE_FINANCIAL_METRICS_TASK RESUME;
ALTER TASK LEASE_OPS_UPDATE_TASK SUSPEND;
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