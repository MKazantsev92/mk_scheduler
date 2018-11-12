create or replace package mk_scheduler_pkg is

  -- Author  : MIKHAIL.KAZANTSEV
  -- Created : 28.10.2018 21:28:24
  -- Purpose : execute custom schedules
  
  -- Public type declarations
  subtype t_scheduler_job is mk_scheduler_jobs%rowtype;
  subtype t_job_id        is mk_scheduler_jobs.job_id%type;
  subtype t_owner         is mk_scheduler_jobs.owner%type;
  subtype t_job_name      is mk_scheduler_jobs.qualified_job_name%type;
  subtype t_is_active     is mk_scheduler_jobs.is_active%type;
  subtype t_error_message is mk_scheduler_jobs.error_message%type;
  
  subtype t_full_job_name is all_scheduler_job_run_details.job_name%type;
  subtype t_ora_job_stat  is all_scheduler_job_run_details.status%type;
  
  subtype t_job_class_name       is all_scheduler_job_classes.job_class_name%type;
  
  subtype t_parameter_nm         is mk_scheduler_jobs_parameters.name%type;
  subtype t_parameter_char_value is mk_scheduler_jobs_parameters.char_value%type;
  subtype t_parameter_num_value  is mk_scheduler_jobs_parameters.num_value%type;
  subtype t_parameter_date_value is mk_scheduler_jobs_parameters.date_value%type;
  subtype t_parameter_bool_value is mk_scheduler_jobs_parameters.bool_value%type;
  
  -- Public constant declarations
  c_is_active_not_active constant char(1 byte) := 'N';
  c_is_active_active     constant char(1 byte) := 'A';
  c_is_active_created    constant char(1 byte) := 'C';
  c_is_active_planned    constant char(1 byte) := 'P';
  c_is_active_running    constant char(1 byte) := 'R';
  c_is_active_error      constant char(1 byte) := 'E';
  c_is_active_finished   constant char(1 byte) := 'F';
  c_is_active_send_kill  constant char(1 byte) := 'S';
  c_is_active_killed     constant char(1 byte) := 'K';
  c_is_active_timeout    constant char(1 byte) := 'T';
  
  c_job_stat_scheduled   constant t_ora_job_stat := 'SCHEDULED';
  c_job_stat_disabled    constant t_ora_job_stat := 'DISABLED';
  c_job_stat_running     constant t_ora_job_stat := 'RUNNING';
  c_job_stat_succeeded   constant t_ora_job_stat := 'SUCCEEDED';
  c_job_stat_failed      constant t_ora_job_stat := 'FAILED';
  c_job_stat_stopped     constant t_ora_job_stat := 'STOPPED';
  
  c_bool_true            constant char(1 byte) := 'Y';
  c_bool_false           constant char(1 byte) := 'N';
  
  c_continuable_true     constant char(1 byte) := 'Y';
  c_continuable_false    constant char(1 byte) := 'N';
  
  c_running_check_true    constant char(1 byte) := 'Y';
  c_running_check_false   constant char(1 byte) := 'N';
  
  -- параметры из таблицы
  c_max_parallel_jobs     constant t_parameter_nm := 'max_parallel_jobs';
  c_max_parallel_lvl_jobs constant t_parameter_nm := 'max_parallel_lvl_jobs';
  
  c_force_stop_running    constant t_parameter_nm := 'force_stop_running';
  c_force_stop_send_kill  constant t_parameter_nm := 'force_stop_send_kill';
  
  c_job_class             constant t_parameter_nm := 'job_class';
  
  c_concurrency_seconds   constant t_parameter_nm := 'concurrency_seconds';
  
  -- параметры по умолчанию, если в таблице параметров не найдено значение
  c_def_max_parallel_jobs     constant t_parameter_num_value := 32;
  c_def_max_parallel_lvl_jobs constant t_parameter_num_value := 32;
  
  c_def_force_stop_running    constant boolean := true;
  c_def_force_stop_send_kill  constant boolean := true;
  
  c_def_job_class             constant t_job_class_name := 'DEFAULT_JOB_CLASS';
  
  c_def_concurrency_seconds   constant t_parameter_num_value := 5;

  -- Public function and procedure declarations
  function create_scheduler_job(
    p_owner              mk_scheduler_jobs.owner%type,
    p_job_name           mk_scheduler_jobs.qualified_job_name%type,
    p_is_active          mk_scheduler_jobs.is_active%type           default null,
    p_restart_count      mk_scheduler_jobs.restart_count%type       default null,
    p_continuable        mk_scheduler_jobs.continuable%type         default null,
    p_level_id           mk_scheduler_jobs.level_id%type            default null,
    p_job_action         mk_scheduler_jobs.job_action%type,
    p_job_comment        mk_scheduler_jobs.job_comment%type)
  return t_scheduler_job;
  
  function check_time_running_sql(p_job_id t_job_id, p_time in date default sysdate)
  return char;
  
  procedure monitor_schedules;
  
  procedure refresh_schedules;
  
  procedure kill_all_scheduler_jobs(p_is_force boolean default true);
  
  procedure start_listener(p_force boolean default false);
  
  procedure stop_listener(p_force boolean default false);
  
  procedure create_listener(p_force_drop boolean default false, p_force_create boolean default false);
  
end mk_scheduler_pkg;
/
create or replace package body mk_scheduler_pkg is
  
  -- Private type declarations
  subtype t_program_name         is all_scheduler_programs.program_name%type;
  subtype t_chain_name           is all_scheduler_chains.chain_name%type;
  subtype t_chain_step_name      is all_scheduler_chain_steps.step_name%type;
  subtype t_chain_rule_name      is all_scheduler_chain_rules.rule_name%type;
  subtype t_schedule_name        is all_scheduler_schedules.schedule_name%type;
--  subtype t_job_class_name       is all_scheduler_job_classes.job_class_name%type;
  subtype t_ora_job_name         is all_scheduler_jobs.job_name%type;
  subtype t_log_history          is all_scheduler_job_classes.log_history%type;
  
  
  -- Private constant declarations
  
  -- параметры из таблицы
  c_prv_program_monitoring  constant t_parameter_nm := 'listener_program_monitoring';
  c_prv_program_refresh     constant t_parameter_nm := 'listener_program_refresh';
  
  c_prv_chain_name          constant t_parameter_nm := 'listener_chain_name';
  
  c_prv_chain_step_1        constant t_parameter_nm := 'listener_chain_step_1';
  c_prv_chain_step_2        constant t_parameter_nm := 'listener_chain_step_2';
  c_prv_chain_step_3        constant t_parameter_nm := 'listener_chain_step_3';
  
  c_prv_chain_rule_1        constant t_parameter_nm := 'listener_chain_rule_1';
  c_prv_chain_rule_2        constant t_parameter_nm := 'listener_chain_rule_2';
  c_prv_chain_rule_3        constant t_parameter_nm := 'listener_chain_rule_3';
  c_prv_chain_rule_4        constant t_parameter_nm := 'listener_chain_rule_4';
  
  c_prv_schedule_name       constant t_parameter_nm := 'listener_schedule_name_15_seconds';
  
  c_prv_job_class_name      constant t_parameter_nm := 'listener_job_class_name';
  
  c_prv_job_name            constant t_parameter_nm := 'listener_job_name';
  
  c_prv_log_history         constant t_parameter_nm := 'listener_log_history';
  
  -- параметры по умолчанию, если в таблице параметров не найдено значение
  c_prv_def_program_monitoring  constant t_program_name    := 'MK_SCHEDULER_LSNR_MONITORING';
  c_prv_def_program_refresh     constant t_program_name    := 'MK_SCHEDULER_LSNR_REFRESH';
  
  c_prv_def_chain_name          constant t_chain_name      := 'MK_SCHEDULER_LSNR_CHAIN';
  
  c_prv_def_chain_step_1        constant t_chain_step_name := 'MONITORING_1';
  c_prv_def_chain_step_2        constant t_chain_step_name := 'REFRESH';
  c_prv_def_chain_step_3        constant t_chain_step_name := 'MONITORING_2';
  
  c_prv_def_chain_rule_1        constant t_chain_rule_name := 'MK_SCHEDULER_LSNR_CHAIN_RULE_1';
  c_prv_def_chain_rule_2        constant t_chain_rule_name := 'MK_SCHEDULER_LSNR_CHAIN_RULE_2';
  c_prv_def_chain_rule_3        constant t_chain_rule_name := 'MK_SCHEDULER_LSNR_CHAIN_RULE_3';
  c_prv_def_chain_rule_4        constant t_chain_rule_name := 'MK_SCHEDULER_LSNR_CHAIN_RULE_4';
  
  c_prv_def_schedule_name       constant t_schedule_name   := 'MK_SCHEDULE_EVERY_15_SECONDS';
  
  c_prv_def_job_class_name      constant t_job_class_name  := 'MK_SCHEDULER_LSNR_JOB_CLASS';
  
  c_prv_def_job_name            constant t_ora_job_name    := 'MK_SCHEDULER_LSNR_JOB';
  
  c_prv_def_log_history         constant t_log_history     := 14;
  
  -- Function and procedure implementations
  
  -- Обновление записи об ошибке
  procedure update_error_message(p_job_id t_job_id, p_error_message t_error_message)
  is
    function update_table(p_job_id t_job_id, p_error_message t_error_message)
    return boolean
    is
    pragma autonomous_transaction;
    begin
      update mk_scheduler_jobs set error_message = p_error_message where job_id = p_job_id;
      commit;
      return true;
    exception when others then 
      rollback;
      return false;
    end;
  begin
    if not update_table(p_job_id, p_error_message) 
      then raise_application_error(-20000, 'error while inserting error message, check job_id: '|| p_job_id);
    end if;
  end;
  
  procedure update_error_message(p_job_id t_job_id)
  is
  begin
    update_error_message(p_job_id, null);
  end;
  
  -- Обновление статуса задания
  function update_scheduler_job_status(p_job_id in t_job_id, p_is_active in t_is_active)
  return boolean
  is
  pragma autonomous_transaction;
  begin
    update mk_scheduler_jobs set is_active = p_is_active where job_id = p_job_id;
    commit;
    --update_error_message(p_job_id);
    return true;
  exception when others then 
    rollback;
    update_error_message(p_job_id, 'cannot update job status: '||sqlerrm);
    return false;
  end;
  
  procedure update_scheduler_job_status(p_job_id in t_job_id, p_is_active in t_is_active)
  is
  begin
    if update_scheduler_job_status(p_job_id, p_is_active) then null; end if;
  end;
  
  -- Получение статуса задания
  function get_scheduler_job_status(p_job_id in t_job_id)
  return t_is_active
  is
    l_return t_is_active;
  begin
    select is_active into l_return from mk_scheduler_jobs where job_id = p_job_id;
    return l_return;
  exception when others then 
    update_error_message(p_job_id, 'cannot get job status: '||sqlerrm);
    return null;
  end;
  
  -- Создание записи из переменных
  function create_scheduler_job(
    p_owner              mk_scheduler_jobs.owner%type,
    p_job_name           mk_scheduler_jobs.qualified_job_name%type,
    p_is_active          mk_scheduler_jobs.is_active%type           default null,
    p_restart_count      mk_scheduler_jobs.restart_count%type       default null,
    p_continuable        mk_scheduler_jobs.continuable%type         default null,
    p_level_id           mk_scheduler_jobs.level_id%type            default null,
    p_job_action         mk_scheduler_jobs.job_action%type,
    p_job_comment        mk_scheduler_jobs.job_comment%type)
  return t_scheduler_job
  is
  l_return t_scheduler_job;
  begin
    l_return.job_id             := mk_scheduler_jobs_seq.nextval;
    l_return.owner              := p_owner;
    l_return.job_name           := p_job_name;
    l_return.is_active          := upper(p_is_active);
    l_return.restart_count      := p_restart_count;
    l_return.continuable        := p_continuable;
    l_return.level_id           := p_level_id;
    l_return.job_action         := p_job_action;
    l_return.job_comment        := p_job_comment;
    return l_return;
  end;
  
  -- Получение записи по job_id
  function get_scheduler_job(p_job_id in t_job_id, p_scheduler_job out t_scheduler_job)
  return boolean
  is
  begin
    select * into p_scheduler_job from mk_scheduler_jobs where job_id = p_job_id;
    return true;
  exception when others then
    update_error_message(p_job_id, 'cannot get scheduler job: '||sqlerrm);
    return false;
  end;
  
  procedure get_scheduler_job(p_job_id in t_job_id, p_scheduler_job out t_scheduler_job)
  is
  begin
    if get_scheduler_job(p_job_id, p_scheduler_job) then null; end if;
  end;
  
  -- Получение полного имени джоба
  function get_scheduler_full_job_name(p_job_id in t_job_id, p_full_job_name out t_full_job_name)
  return boolean
  is
    l_scheduler_job t_scheduler_job;
  begin
    if get_scheduler_job(p_job_id, l_scheduler_job) then
      p_full_job_name := dbms_assert.SCHEMA_NAME(Str => l_scheduler_job.owner)||'.'||l_scheduler_job.qualified_job_name;
    end if;
    return true;
  exception when others then
    update_error_message(p_job_id, 'schema_name not valid: "'||l_scheduler_job.owner||'", '||sqlerrm);
    return false;
  end;
  
  function get_scheduler_full_job_name(p_job_id in t_job_id)
  return t_full_job_name
  is
    l_return t_full_job_name := null;
  begin
    if get_scheduler_full_job_name(p_job_id, l_return) then 
      return l_return;
    end if;
    return l_return;
  end;
  
  -- Получение значения параметра
  function get_parameter_num_value(p_parameter_nm t_parameter_nm)
  return t_parameter_num_value
  is
    l_return t_parameter_num_value := null;
  begin
    select num_value into l_return from mk_scheduler_jobs_parameters where name = p_parameter_nm;
    return l_return;
  exception when others then
    return l_return;
  end;
  
  function get_parameter_char_value(p_parameter_nm t_parameter_nm)
  return t_parameter_char_value
  is
    l_return t_parameter_char_value := null;
  begin
    select char_value into l_return from mk_scheduler_jobs_parameters where name = p_parameter_nm;
    return l_return;
  exception when others then
    return l_return;
  end;
  
  function get_parameter_date_value(p_parameter_nm t_parameter_nm)
  return t_parameter_date_value
  is
    l_return t_parameter_date_value := null;
  begin
    select date_value into l_return from mk_scheduler_jobs_parameters where name = p_parameter_nm;
    return l_return;
  exception when others then
    return l_return;
  end;
  
  function get_parameter_bool_value(p_parameter_nm t_parameter_nm)
  return boolean
  is
    l_bool_value t_parameter_bool_value;
    l_return boolean := null;
  begin
    select bool_value into l_bool_value from mk_scheduler_jobs_parameters where name = p_parameter_nm;
    if l_bool_value = c_bool_true then 
      l_return := true; 
    else 
      l_return := false; 
    end if;
    return l_return;
  exception when others then
    return l_return;
  end;
  
  -- Создание задания
  function insert_scheduler_job (p_scheduler_job t_scheduler_job)
  return boolean
  is
  begin
    insert into mk_scheduler_jobs values p_scheduler_job;
    return true;
  exception when others then
    update_error_message(p_scheduler_job.job_id, 'cannot insert scheduler job: '||sqlerrm);
    return false;
  end;
  
  -- Создание задания в оракловом шедулере
  function create_oracle_job(p_job_id t_job_id)
  return boolean
  is
    e_already_exists exception; --declare a user defined exception
    pragma exception_init(e_already_exists, -27477); --bind the error code to the above 
    l_scheduler_job t_scheduler_job := null;
    l_job_class     t_job_class_name;
  begin
    
    l_job_class := nvl(get_parameter_char_value(c_job_class), c_def_job_class);
    
    if get_scheduler_job(p_job_id, l_scheduler_job) then
      dbms_scheduler.create_job(job_name            => l_scheduler_job.qualified_job_name,
                                job_type            => 'PLSQL_BLOCK',
                                job_class           => l_job_class,
                                job_action          => l_scheduler_job.job_action,
                                enabled             => false,
                                auto_drop           => true);
      update_scheduler_job_status(p_job_id, c_is_active_created);
      return true;
    end if;
    return false;
  exception when e_already_exists then
    update_scheduler_job_status(p_job_id, c_is_active_created);
    return true;
  when others then
    update_scheduler_job_status(p_job_id, c_is_active_error);
    update_error_message(p_job_id, 'cannot create oracle job: '||sqlerrm);
    return false;
  end;
  
  procedure create_oracle_job(p_job_id t_job_id)
  is
  begin
    if create_oracle_job(p_job_id) then null; end if;
  end;
  
  -- Запуск задания (enabled = true)
  function enable_scheduler_job(p_job_id t_job_id)
  return boolean
  is
  begin
    if get_scheduler_job_status(p_job_id) = c_is_active_planned then
      dbms_scheduler.enable(name => get_scheduler_full_job_name(p_job_id));
      update_scheduler_job_status(p_job_id, c_is_active_running);
    else
      update_error_message(p_job_id, 'cannot enable oracle job, status not "'||c_is_active_planned||'"');
    end if;
    return true;
  exception when others then
    return false;
  end;
  
  procedure enable_scheduler_job(p_job_id t_job_id)
  is
  begin
    if enable_scheduler_job(p_job_id) then null; end if;
  end;
  
  -- Получить статус ораклового джоба
  function get_oracle_job_status(p_job_id in t_job_id, p_ora_job_stat out t_ora_job_stat)
  return boolean
  is
    l_scheduler_job t_scheduler_job;
    l_ora_job_stat_act t_ora_job_stat := null;
    l_ora_job_stat_log t_ora_job_stat := null;
  begin
    get_scheduler_job(p_job_id, l_scheduler_job);
    
    begin
      select state status
        into l_ora_job_stat_act
        from all_scheduler_jobs
       where owner = l_scheduler_job.owner
         and job_name = l_scheduler_job.qualified_job_name;
    exception when no_data_found then
      l_ora_job_stat_act := null;
      select status 
        into l_ora_job_stat_log
        from (
        select status
          from all_scheduler_job_run_details
         where owner = l_scheduler_job.owner
           and job_name = l_scheduler_job.qualified_job_name
         order by log_id desc)
       where rownum = 1;
    when others then
      raise;
    end;
    
    p_ora_job_stat := nvl(l_ora_job_stat_act, l_ora_job_stat_log);
    return true;
  exception when others then
    p_ora_job_stat := null;
    update_error_message(p_job_id, 'cannot get oracle job status: '||sqlerrm);
    return false;
  end;
  
  function get_oracle_job_status(p_job_id in t_job_id)
  return t_ora_job_stat
  is
    l_return t_ora_job_stat;
  begin
    if get_oracle_job_status(p_job_id, l_return) then null; end if;
    return l_return;
  end;
  
  -- Получить флаг, разрешено ли работать джобу в это время
  function check_time_running_sql(p_job_id t_job_id, p_time in date default sysdate)
  return char
  is
    l_return char(1 byte) := '0';
  begin
    select 
           case when (p_time - active_from > 0) 
                 and ( 
                       (end_hour = begin_hour)
                    or (end_hour > begin_hour and to_number(to_char(p_time, 'hh24')) between begin_hour and end_hour-1)
                    or (end_hour < begin_hour and (to_number(to_char(p_time, 'hh24')) between begin_hour and 24-1 or to_number(to_char(p_time, 'hh24')) between 0 and end_hour-1))
                     )
                then 'Y'
                else 'N'
           end flg
      into l_return
      from mk_scheduler_jobs t
     where job_id = p_job_id;
    return l_return;
  exception when others then
    update_error_message(p_job_id, 'cannot check job time running: '||sqlerrm);
    return l_return;
  end;
  
  function check_time_running(p_job_id t_job_id, p_time in date default sysdate)
  return boolean
  is
  begin
    if check_time_running_sql(p_job_id, p_time) = c_running_check_true then return true; end if;
    return false;
  end;
  
  -- Проверить конкуренцию запущенных джобов
  function check_concurrency(p_job_id in t_job_id)
  return boolean
  is
    l_cnt          number := 0;
    l_cnt_parallel number := 0;
    l_concurrency_seconds t_parameter_num_value;
  begin
    
    l_concurrency_seconds := nvl(get_parameter_num_value(c_concurrency_seconds), c_def_concurrency_seconds);

    select /*+ ordered */
           count(*) cnt
      into l_cnt
      from mk_scheduler_jobs mk, all_scheduler_running_jobs rj, v$session sw, v$session sl
     where 1=1
       and mk.job_id = p_job_id
       and rj.owner = mk.owner
       and rj.job_name = mk.qualified_job_name
       and rj.session_id = sw.sid
       and sw.sid = sl.final_blocking_session
       and sl.wait_class in ('Application','Concurrency')
       and sl.seconds_in_wait > l_concurrency_seconds;

    select /*+ ordered */
           count(*) cnt
      into l_cnt_parallel
      from mk_scheduler_jobs mk, all_scheduler_running_jobs rj, v$px_session psw, v$session sw, v$session sl
     where 1=1
       and mk.job_id = p_job_id
       and rj.owner = mk.owner
       and rj.job_name = mk.qualified_job_name
       and rj.session_id = psw.qcsid
       and psw.sid = sw.sid
       and sw.sid = sl.final_blocking_session
       and sl.wait_class in ('Application','Concurrency')
       and sl.seconds_in_wait > l_concurrency_seconds;
     
    if (l_cnt > 0 or l_cnt_parallel > 0) then
      return true;
    end if;
    
    return false;
    
  exception when others then
    update_error_message(p_job_id, 'cannot check job concurrency: '||sqlerrm);
    return false;    
  end;
  
  -- Запуск запланированных заданий по этапам
  procedure refresh_schedules
  is
    l_max_parallel_jobs     t_parameter_num_value;
    l_max_parallel_lvl_jobs t_parameter_num_value;
  begin
    
    l_max_parallel_jobs     := nvl(get_parameter_num_value(c_max_parallel_jobs),     c_def_max_parallel_jobs);
    l_max_parallel_lvl_jobs := nvl(get_parameter_num_value(c_max_parallel_lvl_jobs), c_def_max_parallel_lvl_jobs);
    
    -- проверка, что все джобы с уровня отработали и запуск следующего уровня
    for lvl in (with finish_flg_with as (
                  select mksj.*, 
                         case 
                           when  mksj.is_active in c_is_active_finished or 
                                (mksj.is_active = c_is_active_error and mksj.continuable = c_continuable_true)
                             then 1
                           else 0
                         end flg
                    from mk_scheduler_jobs mksj
                ),
                levels_with as (
                  select owner, 
                         job_name, 
                         level_id, 
                         min(flg) min_flg
                    from finish_flg_with 
                   group by owner, 
                            job_name, 
                            level_id)
                select owner, 
                       job_name, 
                       min(level_id) level_id 
                  from levels_with 
                 where min_flg <> 1
                 group by owner, 
                          job_name) loop
      for i in (   with all_running as (select /*+ materialize */ count(*) cnt 
                                          from mk_scheduler_jobs t
                                         where t.is_active = c_is_active_running),
                        level_running as (select /*+ materialize */ count(*) cnt 
                                            from mk_scheduler_jobs t
                                           where t.is_active = c_is_active_running
                                             and t.level_id = lvl.level_id 
                                             and t.owner = lvl.owner
                                           and t.job_name = lvl.job_name)
                select job_id 
                  from mk_scheduler_jobs t, all_running ar, level_running lr
                 where t.is_active = c_is_active_planned 
                   and t.level_id = lvl.level_id 
                   and t.owner = lvl.owner
                   and t.job_name = lvl.job_name
                   and rownum <= l_max_parallel_jobs - ar.cnt
                   and rownum <= l_max_parallel_lvl_jobs - lr.cnt
                   and mk_scheduler_pkg.check_time_running_sql(job_id) = c_running_check_true
                   order by job_id
               ) loop
        if check_time_running(i.job_id) then enable_scheduler_job(i.job_id); end if;
      end loop;
    end loop;
    null;
  end;
  
  -- Убийство задания
  function kill_scheduler_job(p_job_id t_job_id, p_is_force boolean default false)
  return boolean
  is
    l_is_active t_is_active;
    l_full_job_name t_full_job_name;
    l_force_stop_running   boolean;
    l_force_stop_send_kill boolean;
  begin
    
    l_force_stop_running   := nvl(get_parameter_bool_value(c_force_stop_running),   c_def_force_stop_running);
    l_force_stop_send_kill := nvl(get_parameter_bool_value(c_force_stop_send_kill), c_def_force_stop_send_kill);
    
    l_is_active := get_scheduler_job_status(p_job_id);
    l_full_job_name := get_scheduler_full_job_name(p_job_id);
    if (l_is_active = c_is_active_running and not p_is_force) then
      update_scheduler_job_status(p_job_id, c_is_active_send_kill);
      dbms_scheduler.stop_job(job_name => l_full_job_name, force => l_force_stop_running);
    elsif (l_is_active = c_is_active_send_kill or p_is_force) then
      update_scheduler_job_status(p_job_id, c_is_active_send_kill);
      dbms_scheduler.stop_job(job_name => l_full_job_name, force => l_force_stop_send_kill);
    elsif (l_is_active in (c_is_active_created, c_is_active_planned)) then
      dbms_scheduler.stop_job(job_name => l_full_job_name, force => l_force_stop_send_kill);
    else
      update_error_message(p_job_id, 'cannot kill oracle job, status not "'||c_is_active_running||'" or "'||c_is_active_send_kill||'"');
    end if;
    return true;
  exception when others then
    update_error_message(p_job_id, 'cannot kill oracle job: '||sqlerrm);
    return false;
  end;
  
  procedure kill_scheduler_job(p_job_id t_job_id, p_is_force boolean default true)
  is
  begin
    if kill_scheduler_job(p_job_id, p_is_force) then null; end if;
  end;
  
  -- Убийство всех запущенных заданий
  procedure kill_all_scheduler_jobs(p_is_force boolean default true)
  is
  begin
    for i in (select job_id from mk_scheduler_jobs where is_active in (c_is_active_running, c_is_active_send_kill)) loop
      kill_scheduler_job(i.job_id, p_is_force);
    end loop;
  end;
  
  -- Мониторинг работы
  procedure monitor_schedules
  is
  begin
    -- создание и планирование активных джобов
    for i in (select job_id from mk_scheduler_jobs where is_active = c_is_active_active) loop
      if create_oracle_job(i.job_id) then
        -- запланировать только созданные задания
        update_scheduler_job_status(i.job_id, c_is_active_planned);
      end if;        
    end loop;
    
    -- обновление статусов запущенных джобов
    for i in (select job_id from mk_scheduler_jobs where is_active = c_is_active_running) loop
      case get_oracle_job_status(i.job_id)
        when c_job_stat_scheduled then 
          begin
            update_error_message(i.job_id, 'job with RUNNING state have SCHEDULED status, not started yet, check oracle start date');
          end;
        when c_job_stat_disabled then
          begin
            update_scheduler_job_status(i.job_id, c_is_active_error);
            update_error_message(i.job_id, 'job with RUNNING state have DISABLED status');
          end;
        when c_job_stat_running then
          begin
            if check_time_running(i.job_id) then
              update_error_message(i.job_id);
            else
              kill_scheduler_job(i.job_id);
              update_scheduler_job_status(i.job_id, c_is_active_timeout);
              update_error_message(i.job_id, 'job timeout, execution time exceeded, session mark kill');
            end if;
            if not check_concurrency(i.job_id) then
              update_scheduler_job_status(i.job_id, c_is_active_running);
            else
              kill_scheduler_job(i.job_id);
              update_scheduler_job_status(i.job_id, c_is_active_send_kill);
              update_error_message(i.job_id, 'job killed becouse of concurrency');
            end if;
          end;
        when c_job_stat_succeeded then
          begin
            update_scheduler_job_status(i.job_id, c_is_active_finished);
          end;
        when c_job_stat_failed then
          begin
            update_scheduler_job_status(i.job_id, c_is_active_error);
            update_error_message(i.job_id, 'job failed with oracle error');
          end;
        when c_job_stat_stopped then
          begin
            update_scheduler_job_status(i.job_id, c_is_active_killed);
            update_error_message(i.job_id, 'job was stopped');
          end;
        else 
          begin
            update_scheduler_job_status(i.job_id, c_is_active_error);
            update_error_message(i.job_id, 'unknown oracle job status');
          end;
      end case;
    end loop;
    
    -- обновление статусов джобов превысивших время выполнения
    for i in (select job_id from mk_scheduler_jobs where is_active = c_is_active_timeout) loop
      case get_oracle_job_status(i.job_id)
        when c_job_stat_scheduled then 
          begin
            update_error_message(i.job_id, 'job timeout, execution time exceeded, job has not STOPPED status, it will not restart in time');
          end;
        when c_job_stat_disabled then
          begin
            update_error_message(i.job_id, 'job timeout, execution time exceeded, job has not STOPPED status, it will not restart in time');
          end;
        when c_job_stat_running then
          begin
            if check_time_running(i.job_id) then
              update_scheduler_job_status(i.job_id, c_is_active_running);
            else
              kill_scheduler_job(i.job_id);
              update_scheduler_job_status(i.job_id, c_is_active_timeout);
              update_error_message(i.job_id, 'job timeout, execution time exceeded, session mark kill');
            end if;
          end;
        when c_job_stat_succeeded then
          begin
            update_scheduler_job_status(i.job_id, c_is_active_finished);
          end;
        when c_job_stat_failed then
          begin
            update_scheduler_job_status(i.job_id, c_is_active_error);
            update_error_message(i.job_id, 'job failed with oracle error');
          end;
        when c_job_stat_stopped then
          begin
            if check_time_running(i.job_id) then
              update_scheduler_job_status(i.job_id, c_is_active_active);
            else
              update_error_message(i.job_id, 'job timeout, execution time exceeded, successfull stopped');
            end if;
          end;
        else 
          begin
            update_scheduler_job_status(i.job_id, c_is_active_error);
            update_error_message(i.job_id, 'unknown oracle job status');
          end;
      end case;
    end loop;
    
    -- обновление статусов отмеченных к удалению джобов
    for i in (select job_id from mk_scheduler_jobs where is_active = c_is_active_send_kill) loop
      case get_oracle_job_status(i.job_id)
        when c_job_stat_scheduled then 
          begin
            update_scheduler_job_status(i.job_id, c_is_active_killed);
            update_error_message(i.job_id, 'job with SEND KILL state have SCHEDULED status, not started yet, check oracle start date');
          end;
        when c_job_stat_disabled then
          begin
            update_scheduler_job_status(i.job_id, c_is_active_killed);
            update_error_message(i.job_id, 'job with SEND KILL state have DISABLED status');
          end;
        when c_job_stat_running then
          begin
            kill_scheduler_job(i.job_id);
          end;
        when c_job_stat_succeeded then
          begin
            update_scheduler_job_status(i.job_id, c_is_active_finished);
          end;
        when c_job_stat_failed then
          begin
            update_scheduler_job_status(i.job_id, c_is_active_killed);
            update_error_message(i.job_id, 'job failed with oracle error while killing process');
          end;
        when c_job_stat_stopped then
          begin
            update_scheduler_job_status(i.job_id, c_is_active_killed);
            --update_error_message(i.job_id, 'job was stopped');
          end;
        else 
          begin
            update_scheduler_job_status(i.job_id, c_is_active_error);
            update_error_message(i.job_id, 'unknown oracle job status');
          end;
      end case;
    end loop;
    
    -- обновление статусов созданных джобов
    for i in (select job_id from mk_scheduler_jobs where is_active = c_is_active_created) loop
      case get_oracle_job_status(i.job_id)
        when c_job_stat_scheduled then 
          begin
            update_error_message(i.job_id, 'job with CREATED state have SCHEDULED status, not started yet, check oracle start date');
          end;
        when c_job_stat_disabled then
          begin
            update_scheduler_job_status(i.job_id, c_is_active_created);
          end;
        when c_job_stat_running then
          begin
            kill_scheduler_job(i.job_id);
            update_scheduler_job_status(i.job_id, c_is_active_created);
            update_error_message(i.job_id, 'job with CREATED state was RUNNING, job marked to stop');
          end;
        when c_job_stat_succeeded then
          begin
            update_scheduler_job_status(i.job_id, c_is_active_finished);
            update_error_message(i.job_id, 'job with CREATED state is SUCCEEDED, job already finished');
          end;
        when c_job_stat_failed then
          begin
            update_scheduler_job_status(i.job_id, c_is_active_error);
            update_error_message(i.job_id, 'job with CREATED state is FAILED, job was already started and failed');
          end;
        when c_job_stat_stopped then
          begin
            update_scheduler_job_status(i.job_id, c_is_active_killed);
            update_error_message(i.job_id, 'job with CREATED state is STOPPED, job was already started and killed');
          end;
        else 
          begin
            update_scheduler_job_status(i.job_id, c_is_active_error);
            update_error_message(i.job_id, 'unknown oracle job status');
          end;
      end case;
    end loop;
    
    -- обновление статусов запланированных джобов
    for i in (select job_id from mk_scheduler_jobs where is_active = c_is_active_planned) loop
      case get_oracle_job_status(i.job_id)
        when c_job_stat_scheduled then 
          begin
            update_error_message(i.job_id, 'job with PLANNED state have SCHEDULED status, not started yet, check oracle start date');
          end;
        when c_job_stat_disabled then
          begin
            update_scheduler_job_status(i.job_id, c_is_active_planned);
          end;
        when c_job_stat_running then
          begin
            update_scheduler_job_status(i.job_id, c_is_active_planned);
            kill_scheduler_job(i.job_id);
            update_error_message(i.job_id, 'job with PLANNED state was RUNNING, job marked to stop');
          end;
        when c_job_stat_succeeded then
          begin
            update_scheduler_job_status(i.job_id, c_is_active_finished);
            update_error_message(i.job_id, 'job with PLANNED state is SUCCEEDED, job already finished');
          end;
        when c_job_stat_failed then
          begin
            update_scheduler_job_status(i.job_id, c_is_active_error);
            update_error_message(i.job_id, 'job with PLANNED state is FAILED, job was already started and failed');
          end;
        when c_job_stat_stopped then
          begin
            update_scheduler_job_status(i.job_id, c_is_active_killed);
            update_error_message(i.job_id, 'job with PLANNED state is STOPPED, job was already started and killed');
          end;
        else 
          begin
            update_scheduler_job_status(i.job_id, c_is_active_error);
            update_error_message(i.job_id, 'unknown oracle job status');
          end;
      end case;
    end loop;
    null;
  end;
  
  -- статус листнера
  procedure check_listener
  is
  begin
    null;
  end;
  
  -- запуск листнера
  procedure start_listener(p_force boolean default false)
  is
    l_prv_def_job_name t_ora_job_name;
  begin
    l_prv_def_job_name := nvl(get_parameter_char_value(c_prv_job_name), c_prv_def_job_name);

    begin
      dbms_scheduler.enable(l_prv_def_job_name);
    exception when others then
      if not p_force then raise; end if;
    end;
  end;
  
  -- остановка листнера
  procedure stop_listener(p_force boolean default false)
  is
    l_prv_def_job_name t_ora_job_name;
  begin
    l_prv_def_job_name := nvl(get_parameter_char_value(c_prv_job_name), c_prv_def_job_name);
    begin 
      dbms_scheduler.stop_job(l_prv_def_job_name); 
    exception when others then null; 
    end;
    
    begin
      dbms_scheduler.disable(l_prv_def_job_name);
    exception when others then
      if not p_force then raise; end if;
    end;
  end;
  
  -- создание/пересоздание листнера
  -- переписать хардкод имён на параметры
  procedure create_listener(p_force_drop boolean default false, p_force_create boolean default false)
  is
    l_prv_program_monitoring  t_program_name;
    l_prv_program_refresh     t_program_name;
                                 
    l_prv_chain_name          t_chain_name;
                                 
    l_prv_chain_step_1        t_chain_step_name;
    l_prv_chain_step_2        t_chain_step_name;
    l_prv_chain_step_3        t_chain_step_name;
                                 
    l_prv_chain_rule_1        t_chain_rule_name;
    l_prv_chain_rule_2        t_chain_rule_name;
    l_prv_chain_rule_3        t_chain_rule_name;
    l_prv_chain_rule_4        t_chain_rule_name;
                                 
    l_prv_schedule_name       t_schedule_name;
                                 
    l_prv_job_class_name      t_job_class_name;
                                 
    l_prv_job_name            t_ora_job_name;
                                 
    l_prv_log_history         t_log_history;
  begin
    
    l_prv_program_monitoring  := nvl(get_parameter_char_value(c_prv_program_monitoring), c_prv_def_program_monitoring);
    l_prv_program_refresh     := nvl(get_parameter_char_value(c_prv_program_refresh),    c_prv_def_program_refresh);
                                 
    l_prv_chain_name          := nvl(get_parameter_char_value(c_prv_chain_name),         c_prv_def_chain_name);
                                 
    l_prv_chain_step_1        := nvl(get_parameter_char_value(c_prv_chain_step_1),       c_prv_def_chain_step_1);
    l_prv_chain_step_2        := nvl(get_parameter_char_value(c_prv_chain_step_2),       c_prv_def_chain_step_2);
    l_prv_chain_step_3        := nvl(get_parameter_char_value(c_prv_chain_step_3),       c_prv_def_chain_step_3);
                                 
    l_prv_chain_rule_1        := nvl(get_parameter_char_value(c_prv_chain_rule_1),       c_prv_def_chain_rule_1);
    l_prv_chain_rule_2        := nvl(get_parameter_char_value(c_prv_chain_rule_2),       c_prv_def_chain_rule_2);
    l_prv_chain_rule_3        := nvl(get_parameter_char_value(c_prv_chain_rule_3),       c_prv_def_chain_rule_3);
    l_prv_chain_rule_4        := nvl(get_parameter_char_value(c_prv_chain_rule_4),       c_prv_def_chain_rule_4);
                                 
    l_prv_schedule_name       := nvl(get_parameter_char_value(c_prv_schedule_name),      c_prv_def_schedule_name);
                                 
    l_prv_job_class_name      := nvl(get_parameter_char_value(c_prv_job_class_name),     c_prv_def_job_class_name);
                                 
    l_prv_job_name            := nvl(get_parameter_char_value(c_prv_job_name),           c_prv_def_job_name);
                                 
    l_prv_log_history         := nvl(get_parameter_num_value(c_prv_log_history),         c_prv_def_log_history);
    
    -- drop
    begin 
      dbms_scheduler.stop_job(job_name => l_prv_job_name, force => p_force_drop);
    exception when others then
      null;
    end;


    begin 
      dbms_scheduler.disable(name => l_prv_job_name);
    exception when others then 
      if not p_force_drop then raise; end if;
    end;


    begin 
      dbms_scheduler.drop_job(job_name => l_prv_job_name);
    exception when others then
      if not p_force_drop then raise; end if;
    end;

    begin
      dbms_scheduler.drop_job_class(job_class_name => l_prv_job_class_name);
    exception when others then
      if not p_force_drop then raise; end if;
    end;


    begin
      dbms_scheduler.drop_schedule(schedule_name => l_prv_schedule_name);
    exception when others then
      if not p_force_drop then raise; end if;
    end;

    begin
      dbms_scheduler.disable(name => l_prv_chain_name);
    exception when others then
      if not p_force_drop then raise; end if;
    end;


    begin
      dbms_scheduler.drop_chain_rule(chain_name => l_prv_chain_name, rule_name => l_prv_chain_rule_1);
    exception when others then
      if not p_force_drop then raise; end if;
    end;
    begin
      dbms_scheduler.drop_chain_rule(chain_name => l_prv_chain_name, rule_name => l_prv_chain_rule_2);
    exception when others then
      if not p_force_drop then raise; end if;
    end;
    begin
      dbms_scheduler.drop_chain_rule(chain_name => l_prv_chain_name, rule_name => l_prv_chain_rule_3);
    exception when others then
      if not p_force_drop then raise; end if;
    end;
    begin
      dbms_scheduler.drop_chain_rule(chain_name => l_prv_chain_name, rule_name => l_prv_chain_rule_4);
    exception when others then
      if not p_force_drop then raise; end if;
    end;


    begin
      dbms_scheduler.drop_chain_step(chain_name => l_prv_chain_name, step_name => l_prv_chain_step_1);
    exception when others then
      if not p_force_drop then raise; end if;
    end;
    begin
      dbms_scheduler.drop_chain_step(chain_name => l_prv_chain_name, step_name => l_prv_chain_step_2);
    exception when others then
      if not p_force_drop then raise; end if;
    end;
    begin
      dbms_scheduler.drop_chain_step(chain_name => l_prv_chain_name, step_name => l_prv_chain_step_3);
    exception when others then
      if not p_force_drop then raise; end if;
    end;


    begin
      dbms_scheduler.drop_chain(chain_name => l_prv_chain_name);
    exception when others then
      if not p_force_drop then raise; end if;
    end;


    begin
      dbms_scheduler.drop_program(program_name => l_prv_program_refresh);
    exception when others then
      if not p_force_drop then raise; end if;
    end;


    begin
      dbms_scheduler.drop_program(program_name => l_prv_program_monitoring);
    exception when others then
      if not p_force_drop then raise; end if;
    end;

    -- create
    begin
    dbms_scheduler.create_program (
       program_name          => l_prv_program_monitoring,
       program_type          => 'STORED_PROCEDURE',
       program_action        => $$plsql_unit||'.MONITOR_SCHEDULES',
       number_of_arguments   => 0,
       enabled               => TRUE,
       comments              => 'monitorng jobs statuses');
    exception when others then
      if not p_force_create then raise; end if;
    end;


    begin
    dbms_scheduler.create_program (
       program_name          => l_prv_program_refresh,
       program_type          => 'STORED_PROCEDURE',
       program_action        => $$plsql_unit||'.REFRESH_SCHEDULES',
       number_of_arguments   => 0,
       enabled               => TRUE,
       comments              => 'monitorng jobs statuses');
    exception when others then
      if not p_force_create then raise; end if;
    end;


    begin
    dbms_scheduler.create_chain (
       chain_name            =>  l_prv_chain_name,
       rule_set_name         =>  NULL,
       evaluation_interval   =>  NULL,
       comments              =>  'chain for monitoring jobs of mk_scheduler');
    exception when others then
      if not p_force_create then raise; end if;
    end;


    begin
      dbms_scheduler.define_chain_step(chain_name => l_prv_chain_name, step_name => l_prv_chain_step_1, program_name => l_prv_program_monitoring);
    exception when others then
      if not p_force_create then raise; end if;
    end;
    begin
      dbms_scheduler.define_chain_step(chain_name => l_prv_chain_name, step_name => l_prv_chain_step_2,      program_name => l_prv_program_refresh);
    exception when others then
      if not p_force_create then raise; end if;
    end;
    begin
      dbms_scheduler.define_chain_step(chain_name => l_prv_chain_name, step_name => l_prv_chain_step_3, program_name => l_prv_program_monitoring);
    exception when others then
      if not p_force_create then raise; end if;
    end;

    begin
      dbms_scheduler.define_chain_rule(
        chain_name => l_prv_chain_name,
        condition => 'TRUE',
        action => 'START '||l_prv_chain_step_1,
        rule_name => l_prv_chain_rule_1);
    exception when others then
      if not p_force_create then raise; end if;
    end;
    begin
      dbms_scheduler.define_chain_rule(
        chain_name => l_prv_chain_name,
        condition => l_prv_chain_step_1||' SUCCEEDED',
        action => 'START '||l_prv_chain_step_2,
        rule_name => l_prv_chain_rule_2);
    exception when others then
      if not p_force_create then raise; end if;
    end;
    begin
      dbms_scheduler.define_chain_rule(
        chain_name => l_prv_chain_name,
        condition => l_prv_chain_step_2||' SUCCEEDED',
        action => 'START '||l_prv_chain_step_3,
        rule_name => l_prv_chain_rule_3);
    exception when others then
      if not p_force_create then raise; end if;
    end;
    begin
      dbms_scheduler.define_chain_rule(
        chain_name => l_prv_chain_name,
        condition => l_prv_chain_step_3||' SUCCEEDED',
        action => 'END',
        rule_name => l_prv_chain_rule_4);
    exception when others then 
      if not p_force_create then raise; end if;
    end;


    begin
      dbms_scheduler.enable(name => l_prv_chain_name);
    exception when others then
      if not p_force_create then raise; end if;
    end;


    begin
      dbms_scheduler.create_schedule(schedule_name   => l_prv_schedule_name,
                                     start_date      => to_date('01-01-2000 00:00:00', 'dd-mm-yyyy hh24:mi:ss'),
                                     repeat_interval => 'Freq=Secondly;Interval=15',
                                     end_date        => to_date(null),
                                     comments        => 'repeat interval 15 seconds');
    exception when others then
      if not p_force_create then raise; end if;
    end;


    begin
      dbms_scheduler.create_job_class(job_class_name          => l_prv_job_class_name,
--                                      logging_level           => dbms_scheduler.logging_failed_runs,
                                      logging_level           => dbms_scheduler.logging_runs,
                                      log_history             => l_prv_log_history,
                                      comments                => 'mk scheduler jobs class');
    exception when others then
      if not p_force_create then raise; end if;
    end;


    begin
    dbms_scheduler.create_job (
       job_name        => l_prv_job_name,
       job_type        => 'CHAIN',
       job_action      => l_prv_chain_name,
       schedule_name   => l_prv_schedule_name,
       job_class       => l_prv_job_class_name,
       enabled         => FALSE,
       auto_drop       => FALSE);
    exception when others then
      if not p_force_create then raise; end if;
    end;

/*    begin
      dbms_scheduler.enable(name => l_prv_job_name);
    exception when others then
      if not p_force_create then raise; end if;
    end;*/
  
  end;

end mk_scheduler_pkg;
/
