--revoke create job from tctdbs;
revoke create any job from tctdbs;
revoke manage scheduler from tctdbs;
revoke create any rule set from tctdbs;
revoke create any rule from tctdbs;
revoke create any evaluation context from tctdbs;

revoke select on v_$session from tctdbs;
revoke select on v_$px_session from tctdbs;

--grant create job to tctdbs;
grant create any job to tctdbs;
grant manage scheduler to tctdbs;
grant create any rule set to tctdbs;
grant create any rule to tctdbs;
grant create any evaluation context to tctdbs;

grant select on v_$session to tctdbs;
grant select on v_$px_session to tctdbs;

grant dba to mkazantsev;
--revoke dba from mkazantsev;
