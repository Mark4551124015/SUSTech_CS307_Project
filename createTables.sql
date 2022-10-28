create table if not exists  Company
(
    company_id      SERIAL          not null,
    name            varchar(50)     not null
        constraint company_pk primary key
);
create table if not exists City
(
    city_id         SERIAL         not null,

    name            varchar(50) not null
        constraint city_pk primary key
);
create table if not exists PortCity
(
    port_city_id    SERIAL         not null,
    name            varchar(50) not null
        constraint port_city_pk primary key
);
create table if not exists Courier
(
    courier_id      SERIAL             not null ,
    name            varchar(50)      not null
        constraint courier_uq_pk primary key,
    gender          varchar(1)      not null,
    birthday        date            not null,
    phone_number    varchar(50)     not null
        constraint courier_uq_2 unique,
    company     varchar             not null
        constraint courier_fk_1 references Company,
    port_city         varchar            not null
        constraint courier_fk_2 references PortCity
);
create table if not exists Ship
(
    ship_id         SERIAL             not null,
    name            varchar(50)     not null
        constraint ship_pk primary key,
    company      varchar             not null
        constraint ship_fk references Company
);


create table if not exists Container
(
    container_id    SERIAL         not null,
    code            varchar(50) not null
        constraint Container_pk primary key,
    type            varchar(50) not null
);

create table if not exists Shipment
(
    shipment_id     SERIAL             not null,
    item_name       varchar(50)     not null
        constraint shipment_pk primary key,
    item_price      float             not null,
    item_type    varchar            not null,
    from_city    varchar            not null
        constraint shipment_fk_2 references City,
    to_city      varchar            not null
        constraint shipment_fk_3 references City,
    import_city    varchar          not null
        constraint shipment_fk_4 references PortCity,
    export_city     varchar         not null
        constraint shipment_fk_5 references PortCity,
    company         varchar         not null
        constraint shipment_fk_6 references company,
    log_time        timestamp       not null
);
create table if not exists Shipping
(
    shipping_id     SERIAL   not null PRIMARY KEY,
    item_name       varchar(50)     not null
        constraint Shipping_fk references Shipment,
    ship         varchar
        constraint shipping_fk_3 references Ship,
    container   varchar
        constraint shipping_fk_4 references container
);
create table if not exists  Delivery_Retrieval
(
    DR_id           SERIAL         not null,
    item_name       varchar(50)     not null
        constraint delivery_retrieval_fk_1 references shipment,
    type            varchar(50)     not null,
    courier         varchar         not null
        constraint  delivery_retrieval_fk_2 references Courier,
    city            varchar(50)     not null
        constraint  delivery_retrieval_fk_3 references City,
    date            date,
        constraint delivery_retrieval_pk primary key(DR_id),
        constraint delivery_retrieval_uq  unique (item_name,type)

);
create table if not exists import_export_detail
(
    port_id         SERIAL         not null,
    item_name       varchar(50)     not null
        constraint import_export_detail_fk_1 references shipment,
    type            varchar(50) not null,
    item_type       varchar(50)     not null,
    port_city         varchar         not null
        constraint import_export_detail_fk_2 references PortCity,
    tax             float         default(0)      not null,
    date            date        not null,
        constraint import_export_detail_pk primary key (port_id),
        constraint import_export_detail_uq  unique (item_name,type)
);

