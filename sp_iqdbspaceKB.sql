drop procedure if exists "sp_iqdbspaceKB";
;
create procedure "sp_iqdbspaceKB"(
  in "dbspaceName" varchar(128) default null )
result(
  "DBSpaceName" varchar(128),
  "DBSpaceType" varchar(12),
  "Writable" char(1),
  "Online" char(1),
  "UsagePCT" varchar(3),
  "UsageKB" varchar(20),
  "TotalSize" varchar(5),
  "TotalSizeKB" varchar(20),
  "Reserve" varchar(5),
  "NumFiles" integer,
  "NumRWFiles" integer,
  "Stripingon" char(1),
  "StripeSize" varchar(5),
  "BlkTypes" varchar(255),
  "OkToDrop" char(1),
  "lsname" varchar(128),
  "is_dbspace_preallocated" char(1) )
on exception resume
sql security invoker
begin
declare blksz bigint;
  declare local temporary table "iq_dbspace_temp"(
    "FileId" smallint null,
    "RWMode" char(3) null,
    "IQOnline" char(1) null,
    "BlksTotal" unsigned bigint null,
    "BlksInUse" unsigned bigint null,
    "BlksReserve" unsigned bigint null,
    "FirstBlock" unsigned bigint null,
    "BlkSize" unsigned integer null,
    "VerBlks" unsigned bigint null,
    "BlksOldVer" unsigned bigint null,
    "BlksDropAtCP" unsigned bigint null,
    "FLBlks" unsigned bigint null,
    "ROFLBlks" unsigned bigint null,
    "DBIDBlks" unsigned bigint null,
    "CMIDBlks" unsigned bigint null,
    "IABlks" unsigned bigint null,
    "IUBlks" unsigned bigint null,
    "TUBlks" unsigned bigint null,
    "CUBlks" unsigned bigint null,
    "BackupBlks" unsigned bigint null,
    "CPLogBlks" unsigned bigint null,
    "HDRBlks" unsigned bigint null,
    "GFLBlks" unsigned bigint null,
    "SegType" varchar(12) null,
    "CanDrop" char(1) null,
    "isSecNode" char(1) null,
    "PFLBlks" unsigned bigint null,
    "lsid" unsigned bigint null,
    "mirrrorfileid" varchar(128) null,
    "serverID" unsigned integer null,
    "PFLBlksInUse" unsigned bigint null,
    "PFLBlksCommitLog" unsigned bigint null,
    "file_name" long varchar null,
    "IsDASSharedFile" char(1) null,
    ) in "SYSTEM" on commit preserve rows;
  declare local temporary table "iq_dbspace_name_map"(
    "internal_dbspace_name" char(128) not null,
    "external_dbspace_name" char(128) not null,
    ) in "SYSTEM" on commit preserve rows;
  declare "dbspaceName_literal" varchar(128);
  select "str_replace"("dbspaceName",'"',null) into "dbspaceName_literal";
  if not exists(select * from "SYSDBSPACE","SYSDBFILE","SYSIQDBFILE"
      where("dbspace_name" = "dbspaceName_literal" or "dbspaceName_literal" is null)
      and "SYSDBSPACE"."store_type" = 2
      and "SYSDBFILE"."dbspace_id" = "SYSDBSPACE"."dbspace_id"
      and "SYSDBFILE"."dbfile_id" = "SYSIQDBFILE"."dbfile_id"
      and "start_block" > 0) then
    raiserror 17816 'IQ dbspace '''+"dbspaceName"+''' not found.';
    return
  end if;

        select first block_size/512/2 into blksz from SYSIQINFO;

  insert into "iq_dbspace_name_map" select "dbspace_name","dbspace_name" from "SYSDBSPACE" where lower( dbspace_name ) not like 'hotsql_dbspace';
  if("locate"("lcase"("property"('CommandLine')),'-hes') <> 0) then
    update "iq_dbspace_name_map" set "external_dbspace_name" = 'ES_SYSTEM' where "internal_dbspace_name" = 'IQ_SYSTEM_MAIN';
    update "iq_dbspace_name_map" set "external_dbspace_name" = 'ES_TEMP' where "internal_dbspace_name" = 'IQ_SYSTEM_TEMP';
    update "iq_dbspace_name_map" set "external_dbspace_name" = 'ES_DELTA' where "internal_dbspace_name" = 'IQ_SYSTEM_RLV'
  end if;
  execute immediate with quotes on
    'iq utilities main into iq_dbspace_temp dbspace info ' || "dbspaceName";
  select "n"."external_dbspace_name" as "DBSpaceName",
    "min"("SegType") as "DBSpaceType",
    (case when "max"("iqd"."read_write") = 'T' and "max"("RWMode") = 'RW' then 'T' else 'F' end) as "Writable",
    (case when "max"("iqd"."online") = 'T' and "max"("IQOnline") = 'T' then 'T' else 'F' end) as "Online",
    (case when "d"."dbspace_name" = 'IQ_SYSTEM_TEMP' or "max"("isSecNode") = 'F' then convert(varchar(3),"truncnum"("ceiling"("sum"("BlksInUse")*100/"sum"("BlksTotal")),0)) else 'NA' end) as "UsagePCT",
    (case when "d"."dbspace_name" = 'IQ_SYSTEM_TEMP' or "max"("isSecNode") = 'F' then convert ( char(20), "sum"("BlksInUse")*blksz ) else 'NA' end) as "UsageKB",

    convert(varchar(4),"truncnum"("sum"("BlksTotal")*"min"("BlkSize")/"power"(1024,convert(integer,"IsNull"("log"("sum"("BlksTotal")*"min"("BlkSize")),1)/"log"(1024))),2))
     || "substr"('BKMGTP',convert(integer,"IsNull"("log"("sum"("BlksTotal")*"min"("BlkSize")),0)/"log"(1024))+1,1) as "TotalSize",

    convert( unsigned bigint, "sum"("BlksTotal")*"min"("BlkSize")/1024) "TotalSizeKB",

    convert(varchar(4),"truncnum"("sum"("BlksReserve")*"min"("BlkSize")/"power"(1024,convert(integer,"IsNull"("log"("sum"("BlksReserve")*"min"("BlkSize")),1)/"log"(1024))),2))
     || "substr"('BKMGTP',convert(integer,"IsNull"("log"("sum"("BlksReserve")*"min"("BlkSize")),0)/"log"(1024))+1,1) as "Reserve",
    "count"() as "NumFiles",
    "sum"(case when "RWMode" = 'RW' then 1 else 0 end) as "NumRWFiles",
    "striping_on" as "StripingON",
    convert(varchar(4),"truncnum"("iqd"."stripe_size_kb"/"power"(1024,convert(integer,"IsNull"("log"("iqd"."stripe_size_kb"),1)/"log"(1024))),2))
     || "substr"('BKMGTP',convert(integer,"IsNull"("log"("iqd"."stripe_size_kb"*1024),0)/"log"(1024))+1,1) as "StripeSize",
    "replace"(convert(varchar(21),"sum"("HDRBlks")) || 'H','0H','')
     || "Replace"(',' || convert(varchar(21),"sum"("FLBlks")) || 'F',',0F','')
     || "replace"(',' || convert(varchar(21),"sum"("DBIDBlks")) || 'D',',0D','')
     || "replace"(',' || convert(varchar(21),"sum"("VerBlks")) || 'A',',0A','')
     || "replace"(',' || convert(varchar(21),"sum"("BlksOldVer")) || 'O',',0O','')
     || "replace"(',' || convert(varchar(21),"sum"("BlksDropAtCP")) || 'X',',0X','')
     || "replace"(',' || convert(varchar(21),"sum"("CMIDBlks")) || 'M',',0M','')
     || "replace"(',' || convert(varchar(21),"sum"("IABlks")) || 'I',',0I','')
     || "replace"(',' || convert(varchar(21),"sum"("IUBlks")) || 'U',',0U','')
     || "replace"(',' || convert(varchar(21),"sum"("TUBlks")) || 'T',',0T','')
     || "replace"(',' || convert(varchar(21),"sum"("CUBlks")) || 'N',',0N','')
     || "replace"(',' || convert(varchar(21),"sum"("BackupBlks")) || 'B',',0B','')
     || "replace"(',' || convert(varchar(21),"sum"("CPLogBlks")) || 'C',',0C','')
     || "replace"(',' || convert(varchar(21),"sum"("PFLBlks")) || 'R',',0R','')
     || "replace"(',' || convert(varchar(21),"sum"("PFLBlksInUse")) || 'RU',',0RU','')
     || "replace"(',' || convert(varchar(21),"sum"("PFLBlksCommitLog")) || 'RC',',0RC','')
     || "replace"(',' || convert(varchar(21),"sum"("GFLBlks")) || 'G',',0G','') as "BlkTypes",
    if("sum"("VerBlks")+"sum"("BlksOldVer")+"sum"("BlksDropAtCP")+"sum"("GFLBlks")+"sum"("BackupBlks")+"sum"("CPLogBlks")+"sum"("PFLBlks")) = 0
    and "min"("CanDrop") = 'T'
    and not exists(select * from "SYS"."SYSTAB" where "dbspace_id" = "d"."dbspace_id")
    and not exists(select * from "SYS"."SYSIDX" where "dbspace_id" = "d"."dbspace_id")
    and not exists(select * from "SYS"."SYSIQPARTITIONCOLUMN" where "dbspace_id" = "d"."dbspace_id")
    and "d"."dbspace_id" <> 16384
    and "d"."dbspace_id" <> 16385
    and "d"."dbspace_id" <> 32702 then
      'Y' else 'N' endif as "OkToDrop",
    "l"."ls_name" as "Logical Server Name",
    "iqd"."is_dbspace_preallocated"
    from "iq_dbspace_temp"
      join "SYSDBFILE" as "f"
      on "iq_dbspace_temp"."FileId" = "f"."dbfile_id"
      join "SYSDBSPACE" as "d"
      on "f"."dbspace_id" = "d"."dbspace_id"
      join "SYSIQDBSPACE" as "iqd"
      on "iqd"."dbspace_id" = "d"."dbspace_id"
      join "iq_dbspace_name_map" as "n"
      on "d"."dbspace_name" = "n"."internal_dbspace_name"
      left outer join "SYSIQLOGICALSERVER" as "l"
      on "iq_dbspace_temp"."lsid" = "l"."ls_id"
    group by "d"."dbspace_name","n"."external_dbspace_name","d"."dbspace_id","iqd"."stripe_size_kb","striping_on","l"."ls_name","iqd"."is_dbspace_preallocated"
    order by 1 asc;
  drop table "iq_dbspace_temp";
  drop table "iq_dbspace_name_map";
end
;
