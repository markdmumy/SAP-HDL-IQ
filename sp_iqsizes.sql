--
-- all code and scripts are provided as-is
--

drop procedure if exists sp_iqsizes;

create procedure sp_iqsizes(
	in_tbl varchar(255) default '%',
	in_own varchar(255) default '%',
	in_size varchar(2) default 'kb'
)
begin
        declare local temporary table size_res (
                table_owner varchar(128)
                , table_name varchar(128)
                , size double
                , rowcount unsigned bigint
                ) in SYSTEM;
        declare sizeKB double;
        declare rc unsigned bigint;
        declare blksz unsigned int;
        declare size_factor double;
	declare size_name varchar(15);

        select first block_size/512/2 into blksz from SYS.SYSIQINFO;

	if lower ( in_size ) = 'tb' then
		set size_factor = 1024*1024*1024;
	elseif lower ( in_size ) = 'gb' then
		set size_factor = 1024*1024;
	elseif lower ( in_size ) = 'mb' then
		set size_factor = 1024;
	elseif lower ( in_size ) = 'kb' then
		set size_factor = 1;
	else
		message 'in_size must be kb, mb, gb, or tb' to client;
	end if;
	set size_name = 'size in '+upper( in_size );

        FOR FORLOOP as FORCRSR CURSOR FOR select table_name, user_name table_owner
            from sys.systable, sys.sysuser where creator <> 3 and server_type = 'IQ'
		and creator=user_id
		and table_type <> 'PARTITION'
		and table_type <> 'MAT VIEW'
		and lower( suser_name( creator ) ) like lower( in_own )
		and lower( table_name ) like lower ( in_tbl )
        do
                set sizeKB=0;
                execute immediate 'select convert(double, NBlocks * blksz) / convert( double, size_factor ) into sizeKB from sp_iqtablesize(''"'||table_owner||'"."'||table_name||'"'');';
                execute immediate 'select count(*) into rc from "'||table_owner||'"."'||table_name||'";';
                execute immediate 'insert into size_res select "table_owner", "table_name", sizeKB, rc';
        end for;

	message '' to client;
	message '*****  HDLRE Server: ' + @@servername to client;
	message '*****  Owner filter: ' + in_own to client;
	message '***** Object filter: ' + in_tbl to client;
	message '*****   Size Factor: ' + in_size to client;
	message '' to client;
	execute immediate with result set on 'select table_owner, table_name, convert( varchar(60), convert( numeric(20,4), size) ) as ''' + size_name + ''', commas_int( rowcount ) as RowCount from size_res order by 1,2';
end;

--call sp_iqsizes();
--call sp_iqsizes( in_size='kB', in_own = 'dba%');
--call sp_iqsizes( in_size='MB', in_own = 'dba%');
--call sp_iqsizes( in_size='gB', in_own = 'dba%');
--call sp_iqsizes( in_size='tB', in_own = 'dba%');
--call sp_iqsizes(in_size='tb',  in_tbl = 'table_name%' );
--call sp_iqsizes( in_tbl = '%vt%', in_own = '%rep%');
