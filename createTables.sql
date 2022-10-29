create table project1.company
(
    company_id serial,
    name       varchar(50) not null,
    constraint company_pk
        primary key (name)
);

create table project1.city
(
    city_id serial,
    name    varchar(50) not null,
    constraint city_pk
        primary key (name)
);

create table project1.portcity
(
    port_city_id serial,
    name         varchar(50) not null,
    constraint port_city_pk
        primary key (name)
);

create table project1.courier
(
    courier_id   serial,
    name         varchar(50) not null,
    gender       varchar(1)  not null,
    birthday     date        not null,
    phone_number varchar(50) not null,
    company      varchar(50)  not null,
    port_city    varchar(50)  not null,
    constraint courier_uq_pk
        primary key (name),
    constraint courier_uq_2
        unique (phone_number),
    constraint courier_fk_1
        foreign key (company) references project1.company,
    constraint courier_fk_2
        foreign key (port_city) references project1.portcity
);

create table project1.ship
(
    ship_id serial,
    name    varchar(50) not null,
    company varchar(50)  not null,
    constraint ship_pk
        primary key (name),
    constraint ship_fk
        foreign key (company) references project1.company
);

create table project1.container
(
    container_id serial,
    code         varchar(50) not null,
    type         varchar(50) not null,
    constraint container_pk
        primary key (code)
);

create table project1.shipment
(
    shipment_id serial,
    item_name   varchar(50)      not null,
    item_price  double precision not null,
    item_type   varchar(50)       not null,
    from_city   varchar(50)       not null,
    to_city     varchar(50)       not null,
    import_city varchar(50)       not null,
    export_city varchar(50)       not null,
    company     varchar(50)       not null,
    log_time    timestamp        not null,
    constraint shipment_pk
        primary key (item_name),
    constraint shipment_fk_2
        foreign key (from_city) references project1.city(name),
    constraint shipment_fk_3
        foreign key (to_city) references project1.city(name),
    constraint shipment_fk_4
        foreign key (import_city) references project1.portcity(name),
    constraint shipment_fk_5
        foreign key (export_city) references project1.portcity(name),
    constraint shipment_fk_6
        foreign key (company) references project1.company
);

create table project1.shipping
(
    shipping_id serial,
    item_name   varchar(50) not null,
    ship        varchar,
    container   varchar,
    primary key (shipping_id),
    constraint shipping_fk
        foreign key (item_name) references project1.shipment,
    constraint shipping_fk_3
        foreign key (ship) references project1.ship,
    constraint shipping_fk_4
        foreign key (container) references project1.container
);

create table project1.delivery_retrieval
(
    dr_id     serial,
    item_name varchar(50) not null,
    type      varchar(50) not null,
    courier   varchar(50)  not null,
    city      varchar(50) not null,
    date      date,
    constraint delivery_retrieval_pk
        primary key (dr_id),
    constraint delivery_retrieval_uq
        unique (item_name, type),
    constraint delivery_retrieval_fk_1
        foreign key (item_name) references project1.shipment,
    constraint delivery_retrieval_fk_2
        foreign key (courier) references project1.courier,
    constraint delivery_retrieval_fk_3
        foreign key (city) references project1.city(name)
);

create table project1.import_export_detail
(
    port_id   serial,
    item_name varchar(50)                not null,
    type      varchar(50)                not null,
    item_type varchar(50)                not null,
    port_city varchar(50)                 not null,
    tax       double precision default 0 not null,
    date      date                       not null,
    constraint import_export_detail_pk
        primary key (port_id),
    constraint import_export_detail_uq
        unique (item_name, type),
    constraint import_export_detail_fk_1
        foreign key (item_name) references project1.shipment(item_name),
    constraint import_export_detail_fk_2
        foreign key (port_city) references project1.portcity(name)
);

