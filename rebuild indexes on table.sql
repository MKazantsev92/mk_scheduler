insert into mk_scheduler_jobs (job_id, owner, job_name, job_action)
with 
s as (select upper('tctdbs') owner, upper('AUTHORIZATIONS') table_name from dual),
i as ( 
        select index_owner, index_name, partition_name, 'partition' ddl_type
        from all_ind_partitions
        where (index_owner,index_name) in 
           ( select owner, index_name
             from   all_indexes
             where (table_owner, table_name) = (select owner, table_name from s)
           )
        --and status = 'UNUSABLE'
        union all
        select index_owner, index_name, subpartition_name, 'subpartition' ddl_type
        from all_ind_subpartitions
        where (index_owner,index_name) in 
           ( select owner, index_name
             from   all_indexes
             where (table_owner, table_name) = (select owner, table_name from s)
           )
        --and status = 'UNUSABLE'
        union all
        select owner, index_name, null, null
        from all_indexes
        where (table_owner, table_name) = (select owner, table_name from s)
          and partitioned = 'NO'
        --and status = 'UNUSABLE'
    )
select (select nvl(max(job_id), 0) from mk_scheduler_jobs)+rownum job_id,
       'TCTDBS',
       'REBUILD_INDEX_AUTHORIZATIONS',
       case when i.ddl_type is null 
         then 'begin execute immediate ''alter index '||i.index_owner||'.'||i.index_name||' rebuild online parallel 4''; '||'execute immediate ''alter index '||i.index_owner||'.'||i.index_name||' noparallel''; end;'
         else 'begin execute immediate ''alter index '||i.index_owner||'.'||i.index_name||' rebuild '||i.ddl_type||' '||i.partition_name||' online parallel 4''; '||'execute immediate''alter index '||i.index_owner||'.'||i.index_name||' noparallel''; end;'
       end stmt_exec_ddl
  from i
 order by i.index_name, partition_name;

commit;

select t.*, rowid from mk_scheduler_jobs t order by 1;

update mk_scheduler_jobs set is_active = 'A' where job_name = 'REBUILD_INDEX_AUTHORIZATIONS';

commit;
