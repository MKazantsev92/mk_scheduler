begin
dbms_scheduler.create_job(
  job_name => 'TEST_JOB',
  job_type => 'plsql_block',
--  job_action => 'declare c int; begin for i in 1..100000 loop select object_id into c from drop_me where rownum = 1 for update; if mod(i, 30) = 0 then commit; end if; dbms_lock.sleep(1); end loop; end;'
  job_action => 'begin execute immediate ''create index drop_me_idx on drop_me(owner, object_name, object_id) parallel 8''; end;'
);
end;
/

call dbms_scheduler.enable('TEST_JOB');

call dbms_scheduler.stop_job('TEST_JOB');

select * from dba_scheduler_jobs where job_name = 'TEST_JOB';
select * from dba_scheduler_running_jobs;
select * from dba_scheduler_job_run_details where job_name = 'TEST_JOB' order by 1 desc;

select * from v$session where module = 'DBMS_SCHEDULER' and action = 'TEST_JOB';

select * from v$locked_object;
select t.final_blocking_session, t.* from v$session t where status = 'ACTIVE' and sql_id ='1n0kh3gknf51u';
select * from v$px_session;
select * from V$PX_PROCESS;
select * from V$PX_PROCESS_SYSSTAT;
select * from V$PQ_SESSTAT;
select * from v$session where sid in (30, 165);

select sw.sid, sw.module, sw.action, sl.sid, sl.module, sl.action, sl.wait_class, sl.event, sl.seconds_in_wait
  from mk_scheduler_jobs mk, dba_scheduler_running_jobs rj, v$session sw, v$session sl
 where 1=1
   and mk.is_active = 'R'
   and rj.owner = mk.owner
   and rj.job_name = mk.qualified_job_name
   and rj.session_id = sw.sid
   and sw.sid = sl.final_blocking_session
   and sl.wait_class in ('Application','Concurrency')
   and sl.seconds_in_wait > 5
;

select sw.sid, sw.module, sw.action, sl.sid, sl.module, sl.action, sl.wait_class, sl.event, sl.seconds_in_wait
  from mk_scheduler_jobs mk, dba_scheduler_running_jobs rj, v$px_session psw, v$session sw, v$session sl
 where 1=1
   and mk.is_active = 'R'
   and rj.owner = mk.owner
   and rj.job_name = mk.qualified_job_name
   and rj.session_id = psw.qcsid
   and psw.sid = sw.sid
   and sw.sid = sl.final_blocking_session
   and sl.wait_class in ('Application','Concurrency')
   and sl.seconds_in_wait > 5
;

select * from 

select * from v$process where addr = '000007FF19D1ED58';

select * from v$sql where sql_id = '22yx9t77dahvr';
