create or replace procedure sp_iqshowtiered()
begin
  declare is_tiered char(1);
  declare ndx_cnt unsigned bigint;

	select
	rtrim( suser_name( st.creator ) ) as tbl_own
	,rtrim( st.table_name ) as tbl_nm
	,rtrim( si.index_name ) as ndx_nm
	, si.index_type, "unique" as unique_index, ' ' as tiered_index
	into #tier_temp
	from sysindex si, systable st
	where si.table_id = st.table_id and index_type = 'HG'
		and lower(st.table_name) not like 'hdl_hotsql%'
		and lower(st.table_name) not like 't_pwd_history'
	;

	select count(*) into ndx_cnt from #tier_temp;
	message char(10)||'***** Total HG Indexes: '||ndx_cnt to client;

  for FORLOOP as FORCRSR dynamic scroll cursor for select tbl_nm as v_tbl_nm,tbl_own as v_tbl_own,ndx_nm as v_ndx_nm from #tier_temp order by 1,2,3
  do
    select (case when substring(value2,1,1)='N' then 'Y' else 'N' end) into is_tiered from sp_iqindexmetadata( v_ndx_nm , v_tbl_nm , v_tbl_own ) where value1 = 'Maintains Exact Distinct';

    update #tier_temp set tiered_index = isnull( is_tiered, 'L' ) where lower( tbl_nm )= lower( v_tbl_nm )  and lower( tbl_own )= lower( v_tbl_own ) and lower( ndx_nm )= lower( v_ndx_nm );
  end for;

  select count(*) into ndx_cnt from #tier_temp where tiered_index in ('Y','y');
	message '***** Total Tiered Indexes: '||ndx_cnt||char(10) to client;
  select * from #tier_temp where tiered_index in ('Y','y');
end;
