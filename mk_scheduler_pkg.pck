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
  
  c_continuable_true     constant char(1 byte) := 'Y';
  c_continuable_false    constant char(1 byte) := 'N';
  
  c_force_stop_running    constant boolean := true;
  c_force_stop_send_kill  constant boolean := true;
  
  -- переписать на выборку из таблицы
  c_max_parallel_jobs     constant pls_integer := 16;
  c_max_parallel_lvl_jobs constant pls_integer := 4;
  
  -- Public variable declarations
  var t_scheduler_job;

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

  procedure planning_scheduler_jobs;
  
  procedure monitor_schedules;
  
  procedure refresh_schedules;
  
  procedure kill_all_scheduler_jobs(p_is_force boolean default true);
  
  procedure create_listener;
  
end mk_scheduler_pkg;
/
create or replace package body mk_scheduler_pkg is

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
    update_error_message(p_job_id);
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
    l_scheduler_job t_scheduler_job := null;
  begin
    if get_scheduler_job(p_job_id, l_scheduler_job) then
      dbms_scheduler.create_job(job_name            => l_scheduler_job.qualified_job_name,
                                job_type            => 'PLSQL_BLOCK',
                                job_action          => l_scheduler_job.job_action,
                                enabled             => false,
                                auto_drop           => true);
      return true;
    end if;
    update_scheduler_job_status(p_job_id, c_is_active_created);
    return false;
  exception when others then
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
  
  -- Планирование активных заданий
  procedure planning_scheduler_jobs
  is
  begin
    for i in (select t.job_id from mk_scheduler_jobs t where t.is_active = c_is_active_active) loop
      create_oracle_job(i.job_id);
      update_scheduler_job_status(i.job_id, c_is_active_created);
      -- запланировать только созданные задания
      update_scheduler_job_status(i.job_id, c_is_active_planned);
    end loop;
  end;
  
  -- Получить статус ораклового джоба
  function get_oracle_job_status(p_job_id in t_job_id, p_ora_job_stat out t_ora_job_stat)
  return boolean
  is
    l_scheduler_job t_scheduler_job;
    l_ora_job_stat t_ora_job_stat;
  begin
    get_scheduler_job(p_job_id, l_scheduler_job);
    select status 
      into l_ora_job_stat
      from (
    select 1 ord, 0 log_id, owner, job_name, sj.state status
      from all_scheduler_jobs sj
     where owner = l_scheduler_job.owner
       and job_name = l_scheduler_job.qualified_job_name
    union all
    select 2 ord, log_id, owner, job_name, status
      from all_scheduler_job_run_details
     where owner = l_scheduler_job.owner
       and job_name = l_scheduler_job.qualified_job_name
    order by ord, log_id desc
    )
    where rownum = 1;
    p_ora_job_stat := l_ora_job_stat;
    return true;
  exception when others then
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
  
  -- Проверка сессий на блокировки от сессий шедулера (опасные row lock, library cache lock...)
  
  -- Получить флаг, разрешено ли работать джобу в это время
  function check_time_running(p_job_id t_job_id, p_time in date default sysdate)
  return boolean
  is
    l_flg pls_integer;
  begin
    select 
           case when (p_time - active_from > 0) 
                 and ( 
                       (end_hour - begin_hour = 0)
                    or (end_hour - begin_hour > 0 and to_number(to_char(p_time, 'hh24')) between begin_hour and end_hour-1)
                    or (end_hour - begin_hour < 0 and (to_number(to_char(p_time, 'hh24')) between begin_hour and 24-1 or to_number(to_char(p_time, 'hh24')) between 0 and end_hour-1))
                     )
                then 1
                else 0
           end flg
      into l_flg
      from mk_scheduler_jobs t
     where job_id = p_job_id;
     if l_flg = 1 then return true; end if;
     return false;
  exception when others then
    update_error_message(p_job_id, 'cannot check job time running: '||sqlerrm);
    return false;
  end;
  
  -- Запуск запланированных заданий по этапам
  procedure refresh_schedules
  is
  begin
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
      for i in (   with s as (select /*+ materialize */ count(*) cnt from mk_scheduler_jobs where is_active = c_is_active_running)
                 select job_id 
                  from (
                  select row_number() over(order by job_id) rn,
                         count(decode(is_active, 'R', 1, null)) over(partition by level_id) cnt_running,
                         job_id 
                    from mk_scheduler_jobs, s
                   where is_active = c_is_active_planned 
                     and level_id = lvl.level_id 
                     and owner = lvl.owner
                     and job_name = lvl.job_name
                     and rownum <= c_max_parallel_jobs - s.cnt
                   order by job_id) sj_in
                 where rn <= c_max_parallel_lvl_jobs  - cnt_running
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
  begin
    l_is_active := get_scheduler_job_status(p_job_id);
    l_full_job_name := get_scheduler_full_job_name(p_job_id);
    if (l_is_active = c_is_active_running and not p_is_force) then
      update_scheduler_job_status(p_job_id, c_is_active_send_kill);
      dbms_scheduler.stop_job(job_name => l_full_job_name, force => c_force_stop_running);
    elsif (l_is_active = c_is_active_send_kill or p_is_force) then
      update_scheduler_job_status(p_job_id, c_is_active_send_kill);
      dbms_scheduler.stop_job(job_name => l_full_job_name, force => c_force_stop_send_kill);
    elsif (l_is_active in (c_is_active_created, c_is_active_planned)) then
      --update_scheduler_job_status(p_job_id, c_is_active_send_kill);
      dbms_scheduler.stop_job(job_name => l_full_job_name, force => c_force_stop_send_kill);
    else
      update_error_message(p_job_id, 'cannot kill oracle job, status not "'||c_is_active_running||'" or "'||c_is_active_send_kill||'"');
    end if;
    return true;
  exception when others then
    update_error_message(p_job_id, 'cannot kill oracle job: '||sqlerrm);
    return false;
  end;
  
  procedure kill_scheduler_job(p_job_id t_job_id, p_is_force boolean default false)
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
    -- обновление статусов запущенных джобов
    for i in (select job_id from mk_scheduler_jobs where is_active = c_is_active_running) loop
      case get_oracle_job_status(i.job_id)
        when c_job_stat_scheduled then 
          begin
            --update_scheduler_job_status(i.job_id, c_is_active_error);
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
            --update_scheduler_job_status(i.job_id, c_is_active_error);
            update_error_message(i.job_id, 'job timeout, execution time exceeded, job has not STOPPED status, it will not restart in time');
          end;
        when c_job_stat_disabled then
          begin
            --update_scheduler_job_status(i.job_id, c_is_active_error);
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
              update_scheduler_job_status(i.job_id, c_is_active_planned);
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
            --update_scheduler_job_status(i.job_id, c_is_active_send_kill);
            --update_error_message(i.job_id, 'killing process');
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
            update_error_message(i.job_id, 'job was stopped');
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
            --update_scheduler_job_status(i.job_id, c_is_active_error);
            update_error_message(i.job_id, 'job with CREATED state have SCHEDULED status, not started yet, check oracle start date');
          end;
        when c_job_stat_disabled then
          begin
            update_scheduler_job_status(i.job_id, c_is_active_created);
            --update_error_message(i.job_id, 'job with CREATED state have DISABLED status');
          end;
        when c_job_stat_running then
          begin
            update_scheduler_job_status(i.job_id, c_is_active_created);
            update_error_message(i.job_id, 'job with CREATED state was RUNNING, job marked to stop');
            kill_scheduler_job(i.job_id);
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
            --update_scheduler_job_status(i.job_id, c_is_active_error);
            update_error_message(i.job_id, 'job with PLANNED state have SCHEDULED status, not started yet, check oracle start date');
          end;
        when c_job_stat_disabled then
          begin
            update_scheduler_job_status(i.job_id, c_is_active_planned);
            --update_error_message(i.job_id, 'job with PLANNED state have DISABLED status');
          end;
        when c_job_stat_running then
          begin
            update_scheduler_job_status(i.job_id, c_is_active_planned);
            update_error_message(i.job_id, 'job with PLANNED state was RUNNING, job marked to stop');
            kill_scheduler_job(i.job_id);
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
  
  -- временное решение листнера
  -- переделать с бесконечного цикла на задание по расписанию с интервалом 15 секунд
  -- где будут 4 программы запускаться по очереди
  procedure create_listener
  is
  begin
    begin
      dbms_scheduler.stop_job('MK.MK_SCHEDULER_LISTENER');
    exception when others then null;
    end;
    begin
      dbms_scheduler.drop_job('MK.MK_SCHEDULER_LISTENER');
    exception when others then null;
    end;
    begin
      dbms_scheduler.create_job(job_name            => 'MK.MK_SCHEDULER_LISTENER',
                                job_type            => 'PLSQL_BLOCK',
                                job_action          =>             'begin'
                                                        ||chr(10)||'  while true loop'
                                                        ||chr(10)||'  begin'
                                                        ||chr(10)||'    execute immediate ''begin mk_scheduler_pkg.planning_scheduler_jobs; end;'';'
                                                        ||chr(10)||'    dbms_lock.sleep(1);'
                                                        ||chr(10)||'    execute immediate ''begin mk_scheduler_pkg.monitor_schedules; end;'';'
                                                        ||chr(10)||'    dbms_lock.sleep(1);'
                                                        ||chr(10)||'    execute immediate ''begin mk_scheduler_pkg.refresh_schedules; end;'';'
                                                        ||chr(10)||'    dbms_lock.sleep(1);'
                                                        ||chr(10)||'    execute immediate ''begin mk_scheduler_pkg.monitor_schedules; end;'';'
                                                        ||chr(10)||'    dbms_lock.sleep(1 /*15*/);'
--                                                        ||chr(10)||'  exception when others then null;'
                                                        ||chr(10)||'  end;'
                                                        ||chr(10)||'  end loop;'
                                                        ||chr(10)||'end;',
                                enabled             => false,
                                auto_drop           => true);
    end;
    begin
      dbms_scheduler.enable('MK.MK_SCHEDULER_LISTENER');
    exception when others then null;
    end;
  end;

end mk_scheduler_pkg;
/
