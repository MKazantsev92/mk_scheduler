BEGIN
DBMS_SCHEDULER.CREATE_PROGRAM (
   program_name          => 'MK_SCHEDULER_JOBS_MONITORING',
   program_type          => 'STORED_PROCEDURE',
   program_action        => 'mk_scheduler_pkg.monitor_schedules',
   number_of_arguments   => 0,
   enabled               => TRUE,
   comments              => 'monitorng jobs statuses');
END;
/

BEGIN
DBMS_SCHEDULER.CREATE_PROGRAM (
   program_name          => 'MK_SCHEDULER_JOBS_PLANNING',
   program_type          => 'STORED_PROCEDURE',
   program_action        => 'mk_scheduler_pkg.planning_scheduler_jobs',
   number_of_arguments   => 0,
   enabled               => TRUE,
   comments              => 'monitorng jobs statuses');
END;
/

BEGIN
DBMS_SCHEDULER.CREATE_PROGRAM (
   program_name          => 'MK_SCHEDULER_JOBS_REFRESH',
   program_type          => 'STORED_PROCEDURE',
   program_action        => 'mk_scheduler_pkg.refresh_schedules',
   number_of_arguments   => 0,
   enabled               => TRUE,
   comments              => 'monitorng jobs statuses');
END;
/

BEGIN
DBMS_SCHEDULER.CREATE_CHAIN (
   chain_name            =>  'mk_scheduler_chain',
   rule_set_name         =>  NULL,
   evaluation_interval   =>  NULL,
   comments              =>  'chain for monitoring jobs of mk_scheduler');
END;
/
 
--- define three steps for this chain. Referenced programs must be enabled.
BEGIN
  DBMS_SCHEDULER.DEFINE_CHAIN_STEP('mk_scheduler_chain', 'monitoring_1', 'MK_SCHEDULER_JOBS_MONITORING');
  DBMS_SCHEDULER.DEFINE_CHAIN_STEP('mk_scheduler_chain', 'planning',     'MK_SCHEDULER_JOBS_PLANNING');
  DBMS_SCHEDULER.DEFINE_CHAIN_STEP('mk_scheduler_chain', 'monitoring_2', 'MK_SCHEDULER_JOBS_MONITORING');
  DBMS_SCHEDULER.DEFINE_CHAIN_STEP('mk_scheduler_chain', 'refresh',      'MK_SCHEDULER_JOBS_REFRESH');
  DBMS_SCHEDULER.DEFINE_CHAIN_STEP('mk_scheduler_chain', 'monitoring_3', 'MK_SCHEDULER_JOBS_MONITORING');
END;
/

--- define corresponding rules for the chain.
BEGIN
  DBMS_SCHEDULER.DEFINE_CHAIN_RULE('mk_scheduler_chain', 'TRUE', 'START monitoring_1');
  DBMS_SCHEDULER.DEFINE_CHAIN_RULE('mk_scheduler_chain', 'monitoring_1 COMPLETED', 'Start planning');
  DBMS_SCHEDULER.DEFINE_CHAIN_RULE('mk_scheduler_chain', 'planning COMPLETED', 'Start monitoring_2');
  DBMS_SCHEDULER.DEFINE_CHAIN_RULE('mk_scheduler_chain', 'monitoring_2 COMPLETED', 'Start refresh');
  DBMS_SCHEDULER.DEFINE_CHAIN_RULE('mk_scheduler_chain', 'refresh COMPLETED', 'Start monitoring_3');
  DBMS_SCHEDULER.DEFINE_CHAIN_RULE('mk_scheduler_chain', 'monitoring_3 COMPLETED', 'END');
END;
/

--- enable the chain
BEGIN
  DBMS_SCHEDULER.ENABLE('mk_scheduler_chain');
END;
/

BEGIN
  DBMS_SCHEDULER.CREATE_SCHEDULE(schedule_name   => 'SCHEDULE_EVERY_15_SECONDS',
                                 start_date      => to_date('01-01-2000 00:00:00', 'dd-mm-yyyy hh24:mi:ss'),
                                 repeat_interval => 'Freq=Secondly;Interval=15',
                                 end_date        => to_date(null),
                                 comments        => 'repeat interval 15 seconds');
END;
/    

BEGIN
  DBMS_SCHEDULER.CREATE_JOB_CLASS(job_class_name          => 'MK_SCHEDULER_JOB_CLASS_LOG',
                                  logging_level           => DBMS_SCHEDULER.LOGGING_FAILED_RUNS,
                                  log_history             => 90,
                                  comments                => 'mk scheduler jobs class');
END;
/

--- create a chain job to start the chain daily at 1:00 p.m.
BEGIN
DBMS_SCHEDULER.CREATE_JOB (
   job_name        => 'mk_scheduler_job',
   job_type        => 'CHAIN',
   job_action      => 'mk_scheduler_chain',
   schedule_name   => 'SCHEDULE_EVERY_15_SECONDS',
   job_class       => 'MK_SCHEDULER_JOB_CLASS_LOG',
   enabled         => TRUE,
   auto_drop       => FALSE);
END;
/

select * from dba_scheduler_jobs where job_name = 'MK_SCHEDULER_JOB';
select * from dba_scheduler_job_run_details where job_name = 'MK_SCHEDULER_JOB' order by 1 desc;
select * from dba_scheduler_chain_steps;
