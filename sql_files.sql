show databases;
create database ETL;
use ETL;
create table directory (s_no int not null auto_increment primary key ,data_path longblob  ); 
select * from directory;

insert into directory (data_path)
value("D:\\cpp_courses");

update directory set data_path ="D:\\cpp_courses" where s_no=1;

create table output_directory(s_no int not null auto_increment primary key,data_path longblob );
select * from output_directory;

create table file (s_no int not null auto_increment primary key ,data_path longblob  );
select* from file;
