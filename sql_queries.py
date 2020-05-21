import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_house_sales_table_drop = "DROP TABLE IF EXISTS house_sales_staging"
staging_postcodes_table_drop = "DROP TABLE IF EXISTS postcodes_staging"
staging_addresses_table_drop = "DROP TABLE IF EXISTS address_staging"
house_sales_table_drop = "DROP TABLE IF EXISTS house_sales"
addresses_table_drop = "DROP TABLE IF EXISTS address_detail_lkp"
houses_table_drop = "DROP TABLE IF EXISTS house_detail_lkp"
dates_table_drop = "DROP TABLE IF EXISTS dates"

# CREATE TABLES

staging_house_sales_table_create = ("""
    CREATE TABLE IF NOT EXISTS house_sales_staging
        (
            sale_id varchar NOT NULL
            , price float NOT NULL
            , transfer_date varchar NOT NULL
            , postcode varchar NOT NULL
            , property_type varchar NOT NULL
            , new_build varchar NOT NULL
            , duration varchar NOT NULL
            , paon varchar NOT NULL
            , saon varchar NOT NULL
            , street varchar NOT NULL
            , locality varchar NOT NULL
            , town_city varchar NOT NULL
            , county varchar NOT NULL
            , country varchar NOT NULL
            , transaction_type varchar NOT NULL
            , record_status varchar NOT NULL
        )
""")

staging_postcodes_table_create = ("""
    CREATE TABLE IF NOT EXISTS postcodes_staging
        (
            postcode1 varchar NOT NULL
            , postcode2 varchar NOT NULL
            , postcode3 varchar NOT NULL
            , county_nm varchar NOT NULL
            , local_auth_nm varchar NOT NULL
            , ward_nm varchar NOT NULL
            , country_nm varchar NOT NULL
            , region_nm varchar
            , parlmntry_cnstit_nm varchar NOT NULL
            , euro_elctrl_rgn_nm varchar
            , prmry_care_trst_nm varchar
            , lower_super_output_area_nm varchar
            , middle_super_output_area_nm varchar
            , longitude float NOT NULL
            , latitude float NOT NULL
            , last_uploaded varchar NOT NULL
            , location varchar
            , area_class_1 varchar NOT NULL
            , area_class_2 varchar NOT NULL
            , area_class_3 varchar NOT NULL
        )
""")

staging_addresses_table_create = ("""
    CREATE TABLE IF NOT EXISTS addresses_staging
        (
            paon varchar NOT NULL
            , saon varchar NOT NULL
            , street varchar NOT NULL
            , locality varchar NOT NULL
            , town_city varchar NOT NULL
            , county varchar NOT NULL
            , country varchar NOT NULL
            , postcode varchar NOT NULL
        )

""")

house_sales_table_create = ("""
    CREATE TABLE IF NOT EXISTS house_sales
        (
            sale_id varchar NOT NULL
            , house_detail_id int NOT NULL
            , address_id int NOT NULL
            , price float NOT NULL
            , transfer_date date NOT NULL
            , transaction_type varchar NOT NULL
            , PRIMARY KEY (sale_id)
        )
""")

house_table_create = ("""
    CREATE TABLE IF NOT EXISTS house_detail_lkp
        (
            house_detail_id int IDENTITY(0,1) NOT NULL
            , property_type varchar NOT NULL
            , new_build varchar NOT NULL
            , duration varchar NOT NULL
            , PRIMARY KEY (house_detail_ID)
        )
""")

addresses_table_create = ("""
    CREATE TABLE IF NOT EXISTS address_detail_lkp
        (
            address_id int IDENTITY(0,1) NOT NULL
            , paon varchar NOT NULL
            , saon varchar
            , street varchar NOT NULL
            , locality varchar NOT NULL
            , town_city varchar
            , district varchar
            , county varchar
            , country varchar
            , postcode varchar NOT NULL
            , longitude float 
            , latitude float 
            , area_class_1 varchar 
            , area_class_2 varchar 
            , area_class_3 varchar
            , PRIMARY KEY (address_id)
        )
""")

dates_table_create = ("""
    CREATE TABLE IF NOT EXISTS dates
        (
            date_stamp date NOT NULL
            , year int NOT NULL
            , month int NOT NULL
            , week_of_year int NOT NULL
            , day int NOT NULL
            , day_of_week int NOT NULL
            , PRIMARY KEY (date_stamp)
        )
""")

# STAGING TABLES

staging_house_sales_copy = ("""
    COPY house_sales_staging
    FROM {}
    IAM_ROLE {}
    REGION 'eu-west-2'
    CSV;
""").format(config.get('S3','HOUSE_SALES_DATA')
           , config.get('IAM_ROLE','ARN'))

add_house_detail_id_col = ("""
    ALTER TABLE house_sales_staging
    ADD COLUMN house_detail_id int
    DEFAULT 1
""")

add_address_id_col = ("""
    ALTER TABLE house_sales_staging
    ADD COLUMN address_id int
    DEFAULT 1
""")

staging_postcodes_copy = ("""
    COPY postcodes_staging
    FROM {}
    IAM_ROLE {}
    REGION 'eu-west-2'
    CSV
    IGNOREHEADER 1;
""").format(config.get('S3','POSTCODE_DATA')
           , config.get('IAM_ROLE','ARN'))



staging_address_table_insert = ("""
    INSERT INTO addresses_staging(paon, saon, street, locality, town_city,
                                  county, country, postcode)
        SELECT DISTINCT
            paon
            , saon
            , street
            , locality
            , town_city
            , county
            , country
            , postcode
        FROM
            house_sales_staging
""")

# FINAL TABLES

house_sales_table_insert = ("""
    INSERT INTO house_sales(sale_id, house_detail_id, address_id, price,
                            transaction_type, transfer_date)
    SELECT
        sale_id
        , house_detail_id
        , address_id
        , price
        , transaction_type
        , cast(transfer_date as date)
    FROM
        house_sales_staging
""")

house_details_table_insert = ("""
    INSERT INTO house_detail_lkp(property_type, new_build, duration)
    SELECT DISTINCT
        property_type
        , new_build
        , duration
    FROM
        house_sales_staging
""")

address_table_insert = ("""
    INSERT INTO address_detail_lkp(paon, saon, street, locality,town_city,
                                   county, country, postcode, longitude, latitude,
                                   area_class_1, area_class_2, area_class_3)
    SELECT
        a.*
        , p.longitude
        , p.latitude
        , p.area_class_1
        , p.area_class_2
        , p.area_class_3
    FROM addresses_staging AS a
        LEFT JOIN postcodes_staging as p
            ON a.postcode = postcode3
""")

date_table_insert = ("""
    INSERT INTO dates(date_stamp, year, month, week_of_year, day, day_of_week)
    SELECT
        DISTINCT(transfer_date) as transfer_date
        , extract(year from transfer_date)
        , extract(month from transfer_date)
        , extract(week from transfer_date)
        , extract(day from transfer_date)
        , extract(dayofweek from transfer_date)
    FROM
        house_sales
""")

# Update queries for the ids in the house_sales table

house_sales_house_detail_id_update = ("""
    UPDATE house_sales_staging SET house_detail_id = a.house_detail_id
    FROM house_detail_lkp a
    WHERE
        house_sales_staging.property_type = a.property_type AND
        house_sales_staging.new_build = a.new_build AND
        house_sales_staging.duration = a.duration
""")


house_sales_address_id_update = ("""
    UPDATE house_sales_staging SET address_id = a.address_id
    FROM address_detail_lkp a
    WHERE
        house_sales_staging.paon = a.paon AND
        house_sales_staging.saon = a.saon AND
        house_sales_staging.street = a.street AND
        house_sales_staging.locality = a.locality AND
        house_sales_staging.town_city = a.town_city AND
        house_sales_staging.county = a.county AND
        house_sales_staging.country = a.country AND
        house_sales_staging.postcode = a.postcode
""")

# QUERY LISTS
drop_table_queries = [staging_house_sales_table_drop,
                     staging_postcodes_table_drop,
                     staging_addresses_table_drop,
                     house_sales_table_drop,
                     addresses_table_drop,
                     houses_table_drop,
                     dates_table_drop]

create_table_queries = [staging_house_sales_table_create,
                       staging_postcodes_table_create,
                       staging_addresses_table_create,
                       house_sales_table_create,
                       house_table_create,
                       addresses_table_create,
                       dates_table_create]

copy_table_queries = [staging_house_sales_copy,
                     staging_postcodes_copy,
                     add_house_detail_id_col,
                     add_address_id_col]

insert_table_queries = [staging_address_table_insert,
                        house_details_table_insert,
                        house_sales_house_detail_id_update,
                        address_table_insert,
                        house_sales_address_id_update,
                        house_sales_table_insert,
                        date_table_insert]