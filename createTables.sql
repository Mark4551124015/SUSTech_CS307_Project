create table if not exists  Company
(
    company_id      int             not null
        primary key,
    name            varchar(50)     not null
        constraint company_uq unique
);

create table if not exists Courier
(
    courier_id      int             not null
        primary key,
    name            varchar(50)     not null
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
    ship_id         int             not null    primary key,
    name            varchar(50)     not null
        constraint ship_uq unique,
    company_id      int             not null
        constraint ship_fk references Company
);


create table if not exists Container
(
    container_id    int         not null    primary key,
    code            varchar(50) not null
        constraint container_uq unique,
    type            varchar(50) not null
);

create table if not exists City
(
    city_id         int         not null    primary key,
    name            varchar(50) not null
        constraint city_uq unique
);
create table if not exists PortCity
(
    port_city_id    int         not null    primary key,
    name            varchar(50) not null
        constraint portCity_uq unique
);


create table if not exists Shipment
(
    shipment_id     int         not null    primary key,
    item_id         int         not null
        constraint shipment_fk_1 references Item,
    from_city_id    int         not null
        constraint shipment_fk_2 references City,
    to_city_id      int         not null
        constraint shipment_fk_3 references City,


    retrieval_id    int
        constraint shipment_fk_3 references Retrieval,
    export_time     date,
    export_city_id  int
        constraint shipment_fk_4 references PortCity,
    export_tax      int         default (0),

    ship_id         int
        constraint shipment_fk_5 references Ship,
    container_id    int,

    import_time     date,
    import_city_id  int
        constraint shipment_fk_6 references PortCity,
    import_tax      int         default (0),
    delivery_id     int
        constraint shipment_fk_7 references Delivery,
    last_update     date        not null,
    total_time      int

);

create table if not exists Item
(
    item_id         int         not null    primary key,
    name            varchar(50) not null,
    price           int         not null,
    type            varchar(50) not null
);


create table if not exists  Delivery
(
    delivery_id     int         not null    primary key,
    courier_id      int         not null
        constraint  delivery_fk references Courier,
    time            date
);

create table if not exists  Retrieval
(
    retrieval_id    int         not null    primary key,
    courier_id      int         not null
        constraint  retrieval_fk references Courier,
    time            date        not null
);
