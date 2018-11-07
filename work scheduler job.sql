declare
  l_var mk_scheduler_pkg.t_schedule_job;
begin
  l_var :=
  mk_scheduler_pkg.create_schedule_record(
    p_owner              => 't',
    p_job_name           => 't',
--    p_is_active          => 'N',
--    p_restart_count      => 1,
--    p_continuable        => 'Y',
--    p_level_id           => 1,
    p_job_action         => 'begin null; end;',
    p_job_comment        => ''
    );
    dbms_output.put_line(l_var.owner);
end;
/
