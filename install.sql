-- Create objects, package, ant all others needed...

prompt start installation

@"create sql objects.sql"
@"grant scheduler.sql"
@"mk_scheduler_pkg.pck"

prompt install complete