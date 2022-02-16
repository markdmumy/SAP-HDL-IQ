drop function if exists commas;
drop function if exists commas_int;
drop function if exists commas_double;
drop function if exists commas_dbl;

create or replace function commas_int ( i_val bigint )
               returns char(26)
begin
               declare str char(20);
 
               set str = right(repeat(' ',19)+string (i_val),20);       -- ex: -9223372036854775807
               return    replace(replace(substr(str, 1,2) + ',' +
                                                            substr(str, 3,3) + ',' +
                                                            substr(str, 6,3) + ',' +
                                                            substr(str, 9,3) + ',' +
                                                            substr(str,12,3) + ',' +
                                                            substr(str,15,3) + ',' +
                                                            substr(str,18,3),
                                                            ' ,','  '),
                                             '-,',' -');
end;       -- dbo.commas();

grant execute on commas_int to PUBLIC;

create or replace function commas_dbl ( i_val double )
               returns char(512)
begin
               declare str char(20);
 
               set str = right(repeat(' ',19)+string (i_val),20);       -- ex: -9223372036854775807
 
               return    replace(replace(substr(str, 1,2) + ',' +
                                                            substr(str, 3,3) + ',' +
                                                            substr(str, 6,3) + ',' +
                                                            substr(str, 9,3) + ',' +
                                                            substr(str,12,3) + ',' +
                                                            substr(str,15,3) + ',' +
                                                            substr(str,18,3),
                                                            ' ,','  '),
                                             '-,',' -');
end;       -- dbo.commas();
 
grant execute on commas_dbl to PUBLIC;
