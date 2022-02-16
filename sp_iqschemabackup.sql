grant connect to bkup_tbl_test;

drop table if exists bkup_tbl_test.t1;
drop table if exists bkup_tbl_test.t2;
drop table if exists bkup_tbl_test.t3;
drop table if exists bkup_tbl_test.t4;
drop table if exists bkup_tbl_test.t5;

create table bkup_tbl_test.t1 ( a1 int, a2 int );
insert into bkup_tbl_test.t1 values ( 1,1 );
select * into bkup_tbl_test.t2 from bkup_tbl_test.t1;
select * into bkup_tbl_test.t3 from bkup_tbl_test.t1;
select * into bkup_tbl_test.t4 from bkup_tbl_test.t1;
select * into bkup_tbl_test.t5 from bkup_tbl_test.t1;

drop procedure if exists sp_iqschemabackup;
go
create procedure sp_iqschemabackup(
	  in schema_owner varchar(128 )
	, in output_dir varchar(1000)
	, in encryption_key varchar(128) default NULL
	, in cloud_credentials varchar(1000) default NULL
)
begin
	declare tbl_backup varchar(20000);
	declare enc_key varchar(150);

	set enc_key = '';
	if encryption_key is not null
	then
		message 'found a key ' to client;
		set enc_key = 'key '''||encryption_key||'''';
	end if;

	for FOR_LOOP as FOR_CURSOR cursor for
        	select suser_name( creator) as towner, table_name tname
        	from systable
        	where lower ( suser_name( creator ) ) = lower( schema_owner )
        	and table_type = 'BASE'
        	and server_type = 'IQ'
	do
        	set tbl_backup= 'backup table '||towner||'.'||tname
			-- output location
			||' to ''' ||output_dir||'/'||towner||'.'||tname||'/'
			-- output files
			||towner||'.'||tname||'.backup'' '
			-- encyption key
			||enc_key||' '
			-- cloud credentials
			||cloud_credentials||';' ;
		message tbl_backup to client;
		execute immediate tbl_backup;
	end for;
	
	return;
end;
go

--optional to make this an event
drop EVENT if exists schema_backup;
go
CREATE EVENT schema_backup
SCHEDULE START TIME '00:00 AM' EVERY 60 MINUTES
HANDLER
BEGIN
	call sp_iqschemabackup ( 'bkup_tbl_test', 'bb://dummydata/event_backup_test', 'encryption key here', 'CONNECTION_STRING ''connection string for cloud here'' ' );
END;
go
trigger event schema_backup;
go

