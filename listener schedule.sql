
begin
  while true loop
  begin
    execute immediate 'begin mk_scheduler_pkg.planning_scheduler_jobs; end;';
    dbms_lock.sleep(2);
    execute immediate 'begin mk_scheduler_pkg.monitor_schedules; end;';
    dbms_lock.sleep(2);
    execute immediate 'begin mk_scheduler_pkg.refresh_schedules; end;';
    dbms_lock.sleep(2);
    execute immediate 'begin mk_scheduler_pkg.monitor_schedules; end;';
    dbms_lock.sleep(15);
  --exception when others then null;
  end;
  end loop;
end;
/

begin
  dbms_scheduler.create_job(job_name            => 'MK.MK_SCHEDULER_LISTENER',
                            job_type            => 'PLSQL_BLOCK',
                            job_action          =>             'begin'
                                                    ||chr(10)||'  while true loop'
                                                    ||chr(10)||'  begin'
                                                    ||chr(10)||'    execute immediate ''begin mk_scheduler_pkg.planning_scheduler_jobs; end;'';'
                                                    ||chr(10)||'    dbms_lock.sleep(2);'
                                                    ||chr(10)||'    execute immediate ''begin mk_scheduler_pkg.monitor_schedules; end;'';'
                                                    ||chr(10)||'    dbms_lock.sleep(2);'
                                                    ||chr(10)||'    execute immediate ''begin mk_scheduler_pkg.refresh_schedules; end;'';'
                                                    ||chr(10)||'    dbms_lock.sleep(2);'
                                                    ||chr(10)||'    execute immediate ''begin mk_scheduler_pkg.monitor_schedules; end;'';'
                                                    ||chr(10)||'    dbms_lock.sleep(15);'
                                                    ||chr(10)||'  exception when others then null;'
                                                    ||chr(10)||'  end;'
                                                    ||chr(10)||'  end loop;'
                                                    ||chr(10)||'end;',
                            enabled             => false,
                            auto_drop           => true);
end;
/

call dbms_scheduler.enable('MK.MK_SCHEDULER_LISTENER');

begin dbms_scheduler.stop_job('MK.MK_SCHEDULER_LISTENER', force => true); end;
/

begin
  execute immediate 'begin mk_scheduler_pkg.planning_scheduler_jobs; end;';
end;
/

--ORA-27366: job "MK"."MK_SCHEDULER_LISTENER" is not running
--ORA-27475: unknown job "MK"."MK_SCHEDULER_LISTENER"

call dbms_scheduler.stop_job('MK.MK_SCHEDULER_LISTENER');

begin mk_scheduler_pkg.planning_scheduler_jobs; end;
/

begin mk_scheduler_pkg.monitor_schedules; end;
/

select * from dba_scheduler_jobs where job_name = 'MK_SCHEDULER_LISTENER';
select * from dba_scheduler_job_run_details where job_name = 'MK_SCHEDULER_LISTENER' order by 1 desc;
