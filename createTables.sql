create table if not exists  Company
(
    company_id      int             not null
        primary key,
    name            varchar(50)     not null
        constraint company_uq unique
);



create table if not exists Courier
(
    courier_id      int             not null primary key,
    name            varchar(50)     unique not null
        constraint courier_uq_1 unique,
    gender          varchar(1)      not null,
    birthday        date            not null,
    phone_number    varchar(50)     not null
        constraint courier_uq_2 unique,
    company_id      int             not null
        constraint courier_fk references Company
);


create table if not exists Ship
(
    ship_id         int             not null
        constraint ship_pk primary key,
    name            varchar(50)     not null
        constraint ship_uq unique,
    company_id      int             not null
        constraint ship_fk references Company
);


create table if not exists Container
(
    container_id    int         not null
        constraint Container_pk primary key,
    code            varchar(50) not null
        constraint container_uq unique,
    type            varchar(50) not null
);

create table if not exists City
(
    city_id         int         not null
        constraint city_pk primary key,
    name            varchar(50) not null
);

create table if not exists PortCity
(
    port_city_id    int         not null
        constraint port_city_pk primary key,
    name            varchar(50) not null
);



create table if not exists  Delivery_Retrieval
(
    DR_id           int         not null
        constraint dr_pk primary key,
    courier_id      int         not null
        constraint  delivery_fk references Courier,
    type            varchar(50)     not null,
    date            date
);


create table if not exists  Ship_Detail
(
    ship_id         int         not null
        constraint  ship_detail_fk_1 references Ship,
    container_id    int         not null
        constraint  ship_detail_fk_2 references Container,
    constraint ship_detail_pk primary key (ship_id, container_id)
);

create table import_export_detail
(
    port_id         int         not null        primary key,
    type            varchar(50) not null,
    city_id         int         not null
        constraint import_export_detail_fk references PortCity,
    tax             int         default(0)      not null,
    date            date        not null
);

create table if not exists Shipping
(
    shipping_id     int   not null    primary key,
    retrieval_id    int
        constraint shipment_fk_1 references Delivery_Retrieval,
    export_id       int
        constraint shipment_fk_2 references import_export_detail,
    ship_id         int
        constraint shipment_fk_3 references Ship,
    container_id    int
        constraint shipment_fk_4 references container,
    import_id       int
        constraint shipment_fk_5 references import_export_detail,
    delivery_id     int
        constraint shipment_fk_6 references Delivery_Retrieval
);

create table if not exists Shipment
(
    shipment_id     int             not null
        constraint shipment_pk primary key,
    item_name       varchar(50)     not null
        constraint shipment_uq unique,
    item_price      int             not null,
    item_type       varchar(50)     not null,
    from_city_id    int             not null
        constraint shipment_fk_2 references City,
    to_city_id      int             not null
        constraint shipment_fk_3 references City,
    shipping_id     int             not null
        constraint shipment_fk_4 references Shipping,
    log_time        timestamp -        not null,
    total_time      int
);


