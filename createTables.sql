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
    city         varchar            not null
        constraint courier_fk_2 references city
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

create table if not exists  Delivery_Retrieval
(
    DR_id           SERIAL         not null
        constraint dr_pk primary key,
    courier      varchar         not null
        constraint  delivery_fk references Courier,
    type            varchar(50)     not null,
    date            date

);

create table if not exists  ItemType
(
    Item_type_id         SERIAL         not null,
    item_type    varchar(50)         not null
        constraint  item_type_pk primary key
);

create table if not exists  Tax
(
    port_city    varchar         not null
        constraint  tax_fk_1 references portcity,
    item_type       varchar         not null
        constraint tax_fk_2 references ItemType,
    export_tax      float,
    import_tax      float,
    last_update     date        default ('1000-01-01') not null,
    constraint ship_detail_pk primary key (port_city, item_type)
);

create table import_export_detail
(
    port_id         SERIAL         not null        primary key,
    type            varchar(50) not null,
    port_city         varchar         not null
        constraint import_export_detail_fk references PortCity,
    tax             float         default(0)      not null,
    date            date        not null
);

create table if not exists Shipping
(
    shipping_id     SERIAL   not null    primary key,
    retrieval_id    int
        constraint shipping_fk_1 references Delivery_Retrieval,
    export_id       int
        constraint shipping_fk_2 references import_export_detail,
    ship         varchar
        constraint shipping_fk_3 references Ship,
    container   varchar
        constraint shipping_fk_4 references container,
    import_id       int
        constraint shipping_fk_5 references import_export_detail,
    delivery_id     int
        constraint shipping_fk_6 references Delivery_Retrieval
);

create table if not exists Shipment
(
    shipment_id     SERIAL             not null,
    item_name       varchar(50)     not null
        constraint shipment_pk primary key,
    item_price      float             not null,
    item_type    varchar     not null
        constraint shipment_fk_1 references ItemType,
    from_city    varchar             not null
        constraint shipment_fk_2 references City,
    to_city      varchar             not null
        constraint shipment_fk_3 references City,
    shipping_id     int             not null
        constraint shipment_fk_4 references Shipping,
    log_time        timestamp      not null
);
