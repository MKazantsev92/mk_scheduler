--revoke create job from mk;
revoke create any job from mk;
revoke manage scheduler from mk;
revoke create any rule set from mk;
revoke create any rule from mk;
revoke create any evaluation context from mk;

--grant create job to mk;
grant create any job to mk;
grant manage scheduler to mk;
grant create any rule set to mk;
grant create any rule to mk;
grant create any evaluation context to mk;

grant dba to mkazantsev;
revoke dba from mkazantsev;
