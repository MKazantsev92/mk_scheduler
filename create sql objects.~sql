select column_name,
       data_type,
       data_length,
       nullable,
       column_id,
       char_length,
       char_used,
       '  '||lower(column_name)||' '||lower(data_type)||'('||char_length||' '||decode(char_used, 'B', 'byte', 'C', 'char')||')'||','
  from dba_tab_cols
 where table_name = 'DBA_SCHEDULER_JOBS'
   and column_name in ('OWNER','JOB_NAME','JOB_ACTION')
;

drop sequence mk_scheduler_jobs_seq;
drop table mk_scheduler_jobs purge;

create sequence mk_scheduler_jobs_seq;

-- добавить refresh_time
-- добавить start_date
-- добавить work interval
create table mk_scheduler_jobs (
  job_id integer not null constraint mk_scheduler_jobs_pk primary key,
  owner varchar2(128 byte),
  job_name varchar2(128 byte),
  qualified_job_name varchar2(128 byte) generated always as ('MK_SCHEDULER_JOB_'||lpad(to_char(job_id), 13, '0')) virtual,
  is_active char(1 byte) default 'N' not null constraint mk_scheduler_jobs_status check (is_active in ('N','A','C','P','R','E','F','S','K','T')),
  active_from date default sysdate not null,
  begin_hour number(2,0) default 0 not null constraint mk_scheduler_jobs_beg_hr check (begin_hour between 0 and 24),
  end_hour number(2,0) default 0 not null constraint mk_scheduler_jobs_end_hr check (end_hour between 0 and 24),
  restart_count integer default 1 not null,
  continuable char(1 byte) default 'N' not null constraint mk_scheduler_jobs_cont check (continuable in ('Y','N')),
  level_id integer default 1 not null,
  job_action varchar2(4000 byte) not null,
  error_message varchar2(4000 byte),
  job_comment varchar2(4000 byte)
);

create index i_mk_scheduler_jobs_is_act on mk_scheduler_jobs(is_active, job_id);

-- добавить дату старта
-- добавить часы выполнения с .. по ..

select t.*, rowid from mk_scheduler_jobs t;

select * from dba_indexes where table_name = 'MK_SCHEDULER_JOBS';

insert into mk_scheduler_jobs (job_id, owner, job_name, job_action)
values (mk_scheduler_jobs_seq.nextval, 'MK', 'TEST_JOB', 'begin null; end;');

insert into mk_scheduler_jobs (job_id, owner, job_name, continuable, job_action)
values (mk_scheduler_jobs_seq.nextval, 'MK', 'TEST_JOB', 'Y', 'begin null end;');

insert into mk_scheduler_jobs (job_id, owner, job_name, job_action)
values (mk_scheduler_jobs_seq.nextval, 'MK', 'TEST_JOB', 'begin dbms_lock.sleep(30); end;');

insert into mk_scheduler_jobs (job_id, owner, job_name, level_id, job_action)
values (mk_scheduler_jobs_seq.nextval, 'MK', 'TEST_JOB', 2, 'begin null end;');

insert into mk_scheduler_jobs (job_id, owner, job_name, level_id, job_action)
values (mk_scheduler_jobs_seq.nextval, 'MK', 'TEST_JOB', 2, 'begin dbms_lock.sleep(30); end;');

insert into mk_scheduler_jobs (job_id, owner, job_name, level_id, job_action)
values (mk_scheduler_jobs_seq.nextval, 'MK', 'TEST_JOB', 4, 'begin null end;');

insert into mk_scheduler_jobs (job_id, owner, job_name, level_id, job_action)
values (mk_scheduler_jobs_seq.nextval, 'MK', 'TEST_JOB', 4, 'begin dbms_lock.sleep(30); end;');

--


insert into mk_scheduler_jobs (job_id, owner, job_name, job_action)
values (mk_scheduler_jobs_seq.nextval, 'MK', 'TEST_JOB2', 'begin null; end;');

insert into mk_scheduler_jobs (job_id, owner, job_name, continuable, job_action)
values (mk_scheduler_jobs_seq.nextval, 'MK', 'TEST_JOB2', 'Y', 'begin null end;');

insert into mk_scheduler_jobs (job_id, owner, job_name, job_action)
values (mk_scheduler_jobs_seq.nextval, 'MK', 'TEST_JOB2', 'begin dbms_lock.sleep(3600); end;');

insert into mk_scheduler_jobs (job_id, owner, job_name, level_id, job_action)
values (mk_scheduler_jobs_seq.nextval, 'MK', 'TEST_JOB2', 2, 'begin null end;');

insert into mk_scheduler_jobs (job_id, owner, job_name, level_id, job_action)
values (mk_scheduler_jobs_seq.nextval, 'MK', 'TEST_JOB2', 2, 'begin dbms_lock.sleep(30); end;');

insert into mk_scheduler_jobs (job_id, owner, job_name, level_id, job_action)
values (mk_scheduler_jobs_seq.nextval, 'MK', 'TEST_JOB2', 4, 'begin null end;');

insert into mk_scheduler_jobs (job_id, owner, job_name, level_id, job_action)
values (mk_scheduler_jobs_seq.nextval, 'MK', 'TEST_JOB2', 4, 'begin dbms_lock.sleep(30); end;');

commit;

/*
call
dbms_scheduler.create_job(job_name => 'MK.MK_SCHEDULER_JOB_0000000000003',
                          job_type => 'plsql_block',
                          job_action => 'begin error end;'
                          );

call
dbms_scheduler.enable(name => 'MK.MK_SCHEDULER_JOB_0000000000003');
*/

select * from dba_scheduler_jobs where owner = 'MK';

create or replace view mk_scheduler_jobs_v as
with 
srj as (
select owner,
       job_name,
       max(session_id)        keep(dense_rank last order by log_id) session_id,
       max(elapsed_time)      keep(dense_rank last order by log_id) elapsed_time,
       max(slave_process_id)  keep(dense_rank last order by log_id) slave_process_id
  from all_scheduler_running_jobs rj
 group by owner, job_name
),
jrd as (
select owner,
       job_name,
       max(status)            keep(dense_rank last order by log_id) status,
       max(error#)            keep(dense_rank last order by log_id) error#,
       max(actual_start_date) keep(dense_rank last order by log_id) actual_start_date,
       max(run_duration)      keep(dense_rank last order by log_id) run_duration,
       max(session_id)        keep(dense_rank last order by log_id) session_id,
       max(additional_info)   keep(dense_rank last order by log_id) additional_info
  from all_scheduler_job_run_details
 group by owner, job_name
)
select /*+ use_nl(mksj sj srj jrd)*/
       mksj.job_id,
       mksj.level_id,
       mksj.owner owner,
       mksj.job_name,
       decode(mksj.is_active, 'N', 'NOT ACTIVE', 
                              'A', 'ACTIVE - READY TO SCHEDULE', 
                              'C', 'CREATED', 
                              'P', 'PLANNED', 
                              'R', 'RUNNING', 
                              'E', 'ERROR', 
                              'F', 'FINISHED SUCCESSFUL',
                              'S', 'SEND KILL',
                              'K', 'KILLED',
                              'T', 'TIMEOUT') is_active,
       coalesce(sj.state, jrd.status) orcl_status,
       coalesce(srj.elapsed_time, jrd.run_duration) run_duration,
       cast(coalesce(sj.last_start_date, jrd.actual_start_date, mksj.active_from) as date) start_date,
       mksj.begin_hour,
       mksj.end_hour,
       mksj.error_message,
       mksj.restart_count,
       decode(mksj.continuable, 'Y', 'YES: CONTINUE ON FAILURE', 'N', 'NO: ABORT ON FAILURE') continuable,
       mksj.qualified_job_name,
       coalesce(sj.job_action, mksj.job_action) job_action,
       coalesce(to_char(srj.session_id), jrd.session_id) session_id,
       sj.job_creator,
       sj.enabled,
       mksj.job_comment,
       jrd.error#,
       jrd.additional_info,
       srj.slave_process_id,
       sj.run_count,
       sj.retry_count,
       sj.failure_count,
       sj.restartable,
       sj.restart_on_failure
  from mk_scheduler_jobs mksj
  left join all_scheduler_jobs sj
    on sj.owner = mksj.owner
   and sj.job_name = mksj.qualified_job_name
  left join srj
    on srj.owner = mksj.owner
   and srj.job_name = mksj.qualified_job_name
  left join jrd
    on jrd.owner = mksj.owner
   and jrd.job_name = mksj.qualified_job_name
 order by job_id
;

-- написать представление Not_started_becouse_of...

update mk_scheduler_jobs set is_active = 'A';
commit;

--select * from all_scheduler_jobs;

select t.*, rowid from mk_scheduler_jobs t;

select t.* from mk_scheduler_jobs_v t;

begin mk_scheduler_pkg.create_listener; end;
/

--begin mk_scheduler_pkg.kill_all_scheduler_jobs; end;
/
