--
-- all code and scripts are provided as-is
--
-- script will run sp_iqcheckdb on an index by index basis
-- this script is restartable so long as in_create_table is set to "no".  when set to YES, the existing
-- data is removed so as to start fresh.
-- you can specfy table and owner names or wildcards to reduce the checking that is done
-- you can specify any dbcc type: check, verify, allocation, dropleaks
-- all logging goes to DBCC_LOG_TABLE and DBCC_LOGS

drop procedure if exists sp_iqcheckdb_indexes;

create procedure sp_iqcheckdb_indexes(
	in_tbl varchar(255) default '%'
	,in_own varchar(255) default '%'
	,in_dbcc_check_type varchar(32) default 'check'
	,in_create_table varchar(3) default 'no'
)
begin
	declare fullNDX varchar(250);
	declare DBCC_COMMAND varchar(250);
	declare DBCC_ERROR varchar(250);
	declare does_table_exist int;

	if lower ( in_dbcc_check_type ) <> 'allocation'
		and lower ( in_dbcc_check_type ) <> 'check'
		and lower ( in_dbcc_check_type ) <> 'verify'
		and lower ( in_dbcc_check_type ) <> 'dropleaks' then
		message char(10)||'***** Incorrect DBCC type of '||in_dbcc_check_type to client;
		message '***** Must be VERIFY, CHECK, ALLOCATION, or DROPLEAKS'||char(10) to client;
		return;
	end if;

	set does_table_exist=0;
	select count(*) into does_table_exist from systable where lower ( table_name ) = lower ( 'DBCC_LOG_TABLE' ) or lower ( table_name ) = lower ( 'DBCC_LOGS' );

	if lower( in_create_table ) = 'yes' or does_table_exist <> 2 then
		message 'dropping old tables' to client;
		drop table if exists DBCC_LOG_TABLE;
		drop table if exists DBCC_LOGS;
		message 'creating DBCC_LOG_TABLE to log all operations' to client;
		create table DBCC_LOG_TABLE(
			table_owner varchar(250) not null
			, table_name varchar(250) not null
			, index_name varchar(250) not null
			, check_started datetime NULL
			, check_stopped datetime NULL
			, errors_detected varchar(250) NULL
		);
		message 'creating DBCC_LOGS to log all operation details' to client;
		create table DBCC_LOGS ( full_index varchar(250), row_num int, Stat varchar(250), Value varchar(250), Flags varchar(250) );
	else
		message 'reusing DBCC_LOG_TABLE' to client;
	end if;

	message 'capturing table and index info ' to client;

	delete from DBCC_LOG_TABLE where lower ( table_name ) like lower ( in_tbl ) and lower( table_owner ) like lower( in_own );

	select
		rtrim( suser_name( st.creator ) )
		,rtrim( st.table_name )
		,rtrim( si.index_name )
		, convert( datetime, NULL)
		, convert( datetime, NULL)
		, convert( varchar(250), NULL)
	into #DBCC_LOG_TABLE
	from sysindex si, systable st
	where si.table_id = st.table_id
		and lower( st.server_type ) = 'iq'
		and lower ( st.table_name ) like lower ( in_tbl )
		and lower( suser_name( st.creator ) ) like lower( in_own )

	;
	-- performance hack for HDLRE
	insert into DBCC_LOG_TABLE select * from #DBCC_LOG_TABLE ;
	drop table if exists #DBCC_LOG_TABLE;
	commit;

	message 'beginning DBCC run at ', getdate() to client;

        FOR FORLOOP as FORCRSR CURSOR FOR
		select table_owner as CUR_table_owner, table_name as CUR_table_name, index_name as CUR_index_name
		from DBCC_LOG_TABLE
		where ( check_stopped is null or check_stopped <= check_started )
			and lower ( table_owner ) like lower( in_own )
			and lower( table_name ) like lower( in_tbl )
		order by 1,2,3
        do
		set fullNDX='"'||CUR_table_owner||'"."'||CUR_table_name||'"."'||CUR_index_name||'"';

		set DBCC_COMMAND='insert into DBCC_LOGS select '''||fullNDX||''', number(), * from sp_iqcheckdb ( '''||in_dbcc_check_type||' index '||fullNDX||''');' ;

		delete from DBCC_LOGS where full_index = fullNDX;

		message 'executing '||in_dbcc_check_type||' DBCC on index: '||fullNDX to client;

		update DBCC_LOG_TABLE set check_started = getdate()
		where table_owner = CUR_table_owner
			and table_name = CUR_table_name
			and index_name = CUR_index_name;
		commit;

		execute immediate DBCC_COMMAND;

		set DBCC_ERROR='UNKOWN';
		select Value into DBCC_ERROR
		from DBCC_LOGS
		where full_index = fullNDX
			and lower ( Stat ) like '%dbcc status%'
			and lower ( Value ) not like lower( '%Free list duplicates checking skipped on secondary server%' );

		update DBCC_LOG_TABLE set check_stopped = getdate(), errors_detected = DBCC_ERROR
		where table_owner = CUR_table_owner
			and table_name = CUR_table_name
			and index_name = CUR_index_name;
		commit;
        end for;

	message 'stopping DBCC run at ', getdate() to client;
	message '***** Output data is saved to table: DBCC_LOG_TABLE and DBCC_LOGS' to client;
end;
