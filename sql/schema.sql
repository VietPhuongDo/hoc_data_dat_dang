create database if not exists mydb;
use mydb;
create table Users(
    user_id bigint primary key,
    login varchar(255) not null,
    gravatar_id varchar(255),
    url varchar(255),
    avatar_url varchar(255)
);

create table Repositories(
    repo_id bigint primary key,
    name varchar(255),
    url varchar(255)
)