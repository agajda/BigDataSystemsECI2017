loading1 = load 'first.txt' using PigStorage(',') as (user:chararray, url:chararray, id:int);
dump loading1;

loading2 = load 'second.txt' using PigStorage(',') as (url:chararray, rating:int);
dump loading2;

for_each = foreach loading1 generate url, id;
dump for_each;

filter_cmd = filter loading1 by id>8;
dump filter_cmd;

join_cmd = join loading1 by url, loading2 by url;
dump join_cmd;

order_by = ORDER loading2 by rating ASC;
dump order_by;

store order_by into 'output';
