drop procedure if exists sp_hdl_unload_table
go

create procedure sp_hdl_unload_table (
	in tbl_owner varchar(128)
	, in tbl_name varchar(128)
	, in basedir_string long varchar
	, in unload_compressed varchar(128) default 'compress'
	, in cloud_auth_string long varchar default null
	, in col_delimiter varchar(10) default ','
)
begin
	-- all object names are CASE SENSITIVE: events is not the same as Events or EVENTS
	-- if the col_delimiter is 'binary' then a binary unload will be done

	declare str long varchar;
	declare unload_stmt long varchar;
	declare format_str long varchar;

	declare start_rc unsigned bigint;
	declare stop_rc unsigned bigint;
	declare rowsec numeric(30,10);
	declare start_tm datetime;
	declare stop_tm datetime;
	declare rc unsigned bigint;
	declare rt_ms unsigned bigint;
	declare tbl_size numeric(30,10);
	declare binary_unload varchar(5);

	set temporary option quoted_identifier='on';
	set temporary option escape_character='on';
	message '' to client;
	-- check parameters
	if tbl_owner is null or tbl_name is null or basedir_string is null or cloud_auth_string is null
		then
		message '**** All procedure parameters are mandatory *****' to client;
		return( -1 );
	end if;

	-- ***** CHAR(10) in a string is equiv to a carriage return
	-- ***** MESSAGE has a limit in out, may have to use select to see debug statements

        set start_tm= getdate();
        message 'EXTRACT start: ' || start_tm to client;

	execute immediate 'select count(*) into rc from '|| tbl_owner||'.'||tbl_name;

        execute immediate 'SET TEMPORARY OPTION TEMP_EXTRACT_DIRECTORY = '''||basedir_string||'/'||tbl_owner||'.'||tbl_name||'/''';
        execute immediate 'SET TEMPORARY OPTION TEMP_EXTRACT_FILE_PREFIX = '''||tbl_owner||'.'||tbl_name||'_''';
	if lower( col_delimiter ) = 'binary'
		then
		set binary_unload='yes';
        	execute immediate 'SET TEMPORARY OPTION TEMP_EXTRACT_BINARY = ''ON''';
        	execute immediate 'SET TEMPORARY OPTION TEMP_EXTRACT_FILE_EXTENSION = ''gz''';
	else
		set binary_unload='no';
        	execute immediate 'SET TEMPORARY OPTION TEMP_EXTRACT_BINARY = ''OFF''';
        	execute immediate 'SET TEMPORARY OPTION TEMP_EXTRACT_FILE_EXTENSION = ''''';
	end if;
	if lower( unload_compressed ) = 'gz' or lower( unload_compressed ) = 'compress' or lower( unload_compressed ) = 'compressed'
		then
        	execute immediate 'SET TEMPORARY OPTION TEMP_EXTRACT_COMPRESS = ''ON''';
	else
        	execute immediate 'SET TEMPORARY OPTION TEMP_EXTRACT_COMPRESS = ''OFF''';
	end if;
        execute immediate 'SET TEMPORARY OPTION TEMP_EXTRACT_MAX_PARALLEL_DEGREE = ''100'''; -- can't exceed vcpu count
        execute immediate 'SET TEMPORARY OPTION TEMP_EXTRACT_QUOTES = ''ON''';
        execute immediate 'SET TEMPORARY OPTION TEMP_EXTRACT_COLUMN_DELIMITER = '''||col_delimiter||'''';
        execute immediate 'set temporary option TEMP_EXTRACT_CONNECTION_STRING='''||cloud_auth_string||'''';

        execute immediate WITH RESULT SET ON 'select * from '|| tbl_owner||'.'||tbl_name;

        SET TEMPORARY OPTION TEMP_EXTRACT_DIRECTORY =;
        SET TEMPORARY OPTION TEMP_EXTRACT_FILE_PREFIX =;

        set stop_tm = getdate();
        message 'EXTRACT stop: ' || stop_tm to client;
        set rt_ms=datediff( ms, start_tm, stop_tm);
        select Kbytes into tbl_size from dbo.sp_iqtablesize ( tbl_owner||'.'||tbl_name );
        set tbl_size = tbl_size / 1024.0 / 1024.0;

	message 'server:			'||@@servername to client;
	message 'unloaded:		'||basedir_string||'/'||tbl_owner+'.'+tbl_name to client;
	message 'type of unload:		'||unload_compressed to client;
        message 'table size (GB):	'||tbl_size to client;
	message 'rows unloaded:		'||rc to client;
	message 'time to unload(ms):	'||rt_ms to client;
	message 'rows/sec:		'||convert( double, ( rc / convert( double , rt_ms ) ) * 1000.0 ) to client;
	message 'binary unload:		'||binary_unload to client;
	if binary_unload = 'no'
		then
		message '	col delim:	'||col_delimiter to client;
	end if;

end;
go
