drop procedure if exists sp_hdl_load_table
go
set quoted_identifier on
go

create procedure sp_hdl_load_table (
	in tbl_owner varchar(128)
	, in tbl_name varchar(128)
	, in load_type varchar(128) default 'server'
	, in file_string long varchar
	, in cloud_auth_string long varchar default null
	, in col_delimiter varchar(10) default '\x09'
)
begin
	-- all object names are CASE SENSITIVE: events is not the same as Events or EVENTS
	-- column delimiter string MUST include single quotes around it
	--
	-- this proc expects server side loads
	-- this proc expects files to be in HDLFS for HDL, not ADLS or S3
	-- if using ADLS (BB/PB) or AWS (S3) then use the fourth parameter to pass
	--	the full authentication string
	-- assumes tab delimited data with CR at end of line
	-- can handle GZ or not
	--

	declare short_load_stmt long varchar;
	declare load_stmt long varchar;
	declare using_string varchar(128);
	declare col_str long varchar;
	declare comma varchar(10);
	declare format_str long varchar;

	declare start_rc unsigned bigint;
	declare stop_rc unsigned bigint;
	declare start_tm datetime;
	declare stop_tm datetime;
	declare rc unsigned bigint;
	declare rt_ms unsigned bigint;

	set temporary option MAX_QUERY_PARALLELISM = 256;
	--set temporary option max_iq_threads_per_connection = 256;
	--set temporary option max_iq_threads_per_team = 256;

	message '' to client;
	-- check parameters
	if tbl_owner is null or tbl_name is null or file_string is null
		then
		message '**** All procedure parameters are mandatory *****' to client;
		return( -1 );
	end if;

	-- ***** CHAR(10) in a string is equiv to a carriage return
	-- ***** MESSAGE has a limit in out, may have to use select to see debug statements

	-- build column list
	set comma = '';
	set col_str = comma+'';
	FOR FORLOOP as FORCRSR CURSOR FOR select column_name, domain_name from sp_iqcolumn ( tbl_name, tbl_owner )
	do
		-- message column_name to client;
		--set col_str = col_str+comma+column_name+' null( zeros, blanks )';
		if lower( col_delimiter ) = 'binary'
			then
			set col_str = col_str+comma+column_name+' binary with null byte';
		else
			set col_str = col_str+comma+column_name+' NULL( blanks, ''NULL'' )';
			if tbl_name = 'events' and domain_name = 'timestamp'
				then
				set col_str = col_str+' DATETIME( ''yyyymmddhhnnss'' ) , filler(1)'
			end if;
		end if;
		set comma=char(10)+','
	end for;
	--message col_str to client;

	-- build format string: either csv or parquet
	-- if the col_delimiter is parquet then its a parquet file.  otherwise csv.

	-- default is server side load
	set using_string = 'using file';

	if lower( col_delimiter ) = 'parquet'
		then
		message '**** building parquet file format *****' to client;
		--select '**** building parquet file format *****';
                set format_str = 'FORMAT parquet'+char(10);
		if lower( load_type ) = 'client'
		then
			message '**** ERROR: PARQUET data cannot be loaded via a client load statement, user a server side load *****' to client;
			return;
		end if;
	elseif lower( col_delimiter ) = 'binary'
		then
		message '**** building binary file format *****' to client;
                set format_str = 'FORMAT binary'+char(10);
	else
		message '**** building csv file format *****' to client;
		--select '**** building csv file format *****';
                set format_str = 'FORMAT csv'+char(10)+
                'DELIMITED BY '''+col_delimiter+''''+char(10)+
                'ROW DELIMITED BY ''\n'''+char(10);
		if lower( load_type ) = 'client'
		then
			set using_string = 'using client file';
		end if;
	end if;


	-- build load statement
	set short_load_stmt = 'load table "' +
		tbl_owner + '"."' + tbl_name + '" (' + char(10) +
		col_str + char(10) +
		')' + char(10) +
		using_string + ' ' + 'no files listed' +char(10)+
		cloud_auth_string +char(10)+
		format_str+char(10)+
                --'limit 1000'+char(10)+
                'QUOTES ON'+char(10)+
                'ESCAPES OFF;'
	;
	-- build load statement
	set load_stmt = 'load table "' +
		tbl_owner + '"."' + tbl_name + '" (' + char(10) +
		col_str + char(10) +
		')' + char(10) +
		using_string + ' ' + file_string +char(10)+
		cloud_auth_string +char(10)+
		format_str+char(10)+
                --'limit 1000'+char(10)+
                'QUOTES ON'+char(10)+
                'ESCAPES OFF;'
	;

	--message short_load_stmt to client;
	--message load_stmt to client;

	-- need this just in case TDS is used.  this will escape the CR/LF in the LOAD stmt
	-- loads can fail without this.
	set temporary option escape_character='on';

	-- run load statement
	execute immediate 'select count(*) into start_rc from '+tbl_owner+'.'+tbl_name;
	set start_tm = getdate();
	--message short_load_stmt to client;
	message '**** loading data now: ', getdate(), ' *****' to client;
	execute immediate load_stmt;
	--waitfor delay '00:00:01';
	set stop_tm = getdate();
	execute immediate 'select count(*) into stop_rc from '+tbl_owner+'.'+tbl_name;

	-- generate stats
	set rc = stop_rc-start_rc;
	set rt_ms = datediff ( ms, start_tm,stop_tm ) ;

	message 'server:			'||@@servername to client;
	message 'loaded:			'||tbl_owner+'.'+tbl_name to client;
	message 'type of load:		'||load_type to client;
	message 'rows loaded:		'||rc to client;
	message 'time to load(ms):	'||rt_ms to client;
	message 'rows/sec:		'||convert( double, ( rc / convert( double , rt_ms ) ) * 1000.0 ) to client;

end;
go
