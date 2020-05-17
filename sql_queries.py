import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_house_sales_table_drop = "DROP TABLE IF EXISTS house_sales_staging"
staging_postcodes_table_drop = "DROP TABLE IF EXISTS postcodes_staging"
house_sales_table_drop = "DROP TABLE IF EXISTS house_sales"
addresses_table_drop = "DROP TABLE IF EXISTS addresses"
houses_table_drop = "DROP TABLE IF EXISTS houses_detail"
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
            , freehold varchar NOT NULL
            , number varchar NOT NULL
            , flat_number varchar NOT NULL
            , street varchar NOT NULL
            , neighbourhood varchar NOT NULL
            , city varchar NOT NULL
            , district varchar NOT NULL
            , region varchar NOT NULL
            , record_status varchar NOT NULL
            , unknown varchar NOT NULL
        )
""")

# you might wonder why this is so large
# it is because this is what the file is like, and post staging we pick out what we want
staging_postcodes_table_create = ("""
    CREATE TABLE IF NOT EXISTS postcodes_staging
        (
            postcode1 varchar NOT NULL
            , postcode2 varchar NOT NULL
            , postcode3 varchar NOT NULL
            , date_intro varchar NOT NULL
            , user_type int NOT NULL 
            , easting varchar NOT NULL
            , northing varchar NOT NULL
            , positional_quality int NOT NULL
            , county_code varchar NOT NULL
            , county_name varchar
            , local_auth_code varchar
            , local_auth_name varchar
            , ward_code varchar
            , ward_name varchar
            , country_code varchar NOT NULL
            , country_name varchar NOT NULL
            , region_code varchar NOT NULL
            , region_name varchar NOT NULL
            , prlmntry_cnst_code varchar NOT NULL
            , prlmntry_cnst_name varchar NOT NULL
            , euro_elctrl_rgn_code varchar
            , euro_elctrl_rgn_name varchar
            , primary_care_trust_code varchar
            , primary_care_trust_name varchar
            , lower_super_output_area_code varchar
            , lower_super_output_area_name varchar
            , middle_super_output_area_code varchar
            , middle_super_output_area_name varchar
            , output_area_class_code varchar
            , output_area_class_name varchar
            , longitude float NOT NULL
            , latitude float NOT NULL
            , spatial_acc varchar
            , last_upload varchar
            , location varchar
            , socrata_id varchar
            , lower_layer_super_output_area varchar
            , ward varchar
        )
""")

house_sales_table_create = ("""
    CREATE TABLE IF NOT EXISTS house_sales
        (
            sale_id int IDENTITY(0,1) NOT NULL
            , house_detail_id int NOT NULL
            , address_id int NOT NULL
            , price float NOT NULL
            , transfer_date date NOT NULL
            , PRIMARY KEY (sale_id)
        )
""")

house_table_create = ("""
    CREATE TABLE IF NOT EXISTS houses_detail
        (
            house_detail_id int IDENTITY(0,1) NOT NULL
            , property_type varchar NOT NULL
            , new_build varchar NOT NULL
            , freehold varchar NOT NULL
            , PRIMARY KEY (house_detail_ID)
        )
""")

addresses_table_create = ("""
    CREATE TABLE IF NOT EXISTS addresses
        (
            address_id int IDENTITY(0,1) NOT NULL
            , prefix varchar
            , flat_num int
            , street varchar NOT NULL
            , neighbourhood varchar NOT NULL
            , town varchar NOT NULL
            , city varchar NOT NULL
            , postcode varchar NOT NULL
            , longitude float NOT NULL
            , latitude float NOT NULL
            , area_classification varchar NOT NULL
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

staging_postcodes_copy = ("""
    COPY postcodes_staging
    FROM {}
    IAM_ROLE {}
    REGION 'eu-west-2'
    CSV
    IGNOREHEADER 1;
""").format(config.get('S3','POSTCODE_DATA')
           , config.get('IAM_ROLE','ARN'))


# FINAL TABLES

# this one last because depends on IDs from others
house_sales_table_insert = ("""
    INSERT INTO house_sales(house_detail_id, address_id, price, transfer_date)
    SELECT
        1
        , 1
        , price
        , cast(transfer_date as date)
    FROM
        house_sales_staging
""")

house_details_table_insert = ("""
    INSERT INTO houses_detail(property_type, new_build, freehold)
    SELECT
        property_type
        , new_build
        , freehold
    FROM
        house_sales_staging
""")

address_table_insert = ("""
    INSERT INTO addresses(prefix, flat_num, street, neighbourhood,
                            town, city, postcode, longitude, latitude,
                            area_classification)
    SELECT
        h.number
        , 1
        , h.street
        , h.neighbourhood
        , h.city
        , h.city
        , h.postcode
        , p.longitude
        , p.latitude
        , p.output_area_class_name
    FROM
        house_sales_staging h
        , postcodes_staging p
    WHERE
        h.postcode=p.postcode1
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



# QUERY LISTS
drop_table_queries = [staging_house_sales_table_drop,
                     staging_postcodes_table_drop,
                     house_sales_table_drop,
                     addresses_table_drop,
                     houses_table_drop,
                     dates_table_drop]

create_table_queries = [staging_house_sales_table_create,
                       staging_postcodes_table_create,
                       house_sales_table_create,
                       house_table_create,
                       addresses_table_create,
                       dates_table_create]

copy_table_queries = [staging_house_sales_copy,
                     staging_postcodes_copy]

insert_table_queries = [house_sales_table_insert,
                       house_details_table_insert,
                       address_table_insert,
                       date_table_insert]