CREATE EXTENSION IF NOT EXISTS file_fdw;
 
CREATE SERVER sa_car_sales_offline FOREIGN DATA WRAPPER file_fdw;
 
CREATE SCHEMA IF NOT EXISTS sa_car_sales_offline;
 
CREATE FOREIGN TABLE sa_car_sales_offline.ext_car_sales_offline(
	id VARCHAR(250),
	date VARCHAR(250),
	salespersonid VARCHAR(250),
	salesperson VARCHAR(250),
    customername VARCHAR(250),
    customercountry VARCHAR(250),
    customerstate VARCHAR(250),
    customerpostalcode VARCHAR(250),
    customercity VARCHAR(250),
    customerstreetname VARCHAR(250),
    customerbuildingnumber VARCHAR(250),
    customertelephonenumber VARCHAR(250),
    customeremail VARCHAR(250),
    carmake VARCHAR(250),
    carmodel VARCHAR(250),
    caryear VARCHAR(250),
    saleprice VARCHAR(250),
    commissionrate VARCHAR(250),
    commissionearned VARCHAR(250),
    paymentmethod VARCHAR(250),
    saleschannel VARCHAR(250),
    cardelername VARCHAR(250)
)
SERVER sa_car_sales_offline
OPTIONS(
	filename 'C:\Program Files\PostgreSQL\17\data\csv_data\car_sales_offline.csv',
	format 'csv',
	header 'true',
	delimiter ';',
	null '',
	escape '"'
);
 
CREATE TABLE sa_car_sales_offline.src_car_sales_offline(
	id VARCHAR(250),
	date VARCHAR(250),
	salespersonid VARCHAR(250),
	salesperson VARCHAR(250),
    customername VARCHAR(250),
    customercountry VARCHAR(250),
    customerstate VARCHAR(250),
    customerpostalcode VARCHAR(250),
    customercity VARCHAR(250),
    customerstreetname VARCHAR(250),
    customerbuildingnumber VARCHAR(250),
    customertelephonenumber VARCHAR(250),
    customeremail VARCHAR(250),
    carmake VARCHAR(250),
    carmodel VARCHAR(250),
    caryear VARCHAR(250),
    saleprice VARCHAR(250),
    commissionrate VARCHAR(250),
    commissionearned VARCHAR(250),
    paymentmethod VARCHAR(250),
    saleschannel VARCHAR(250),
    cardelername VARCHAR(250)
);
 
INSERT INTO sa_car_sales_offline.src_car_sales_offline (
    id, date, salespersonid, salesperson, customername,
    customercountry, customerstate, customerpostalcode, customercity,
    customerstreetname, customerbuildingnumber, customertelephonenumber,
    customeremail, carmake, carmodel, caryear, saleprice, commissionrate,
    commissionearned, paymentmethod, saleschannel, cardelername
)
SELECT
    id, date, salespersonid, salesperson, customername,
    customercountry, customerstate, customerpostalcode, customercity,
    customerstreetname, customerbuildingnumber, customertelephonenumber,
    customeremail, carmake, carmodel, caryear, saleprice, commissionrate,
    commissionearned, paymentmethod, saleschannel, cardelername
FROM sa_car_sales_offline.ext_car_sales_offline;
 
 
-- SELECT * FROM sa_car_sales_offline.ext_car_sales_offline LIMIT 5;
-- SELECT * FROM sa_car_sales_offline.src_car_sales_offline LIMIT 5;
-- SELECT COUNT(*) FROM sa_car_sales_offline.src_car_sales_offline;
 
 
CREATE SERVER sa_car_sales_online FOREIGN DATA WRAPPER file_fdw;
 
CREATE SCHEMA IF NOT EXISTS sa_car_sales_online;
 
CREATE FOREIGN TABLE sa_car_sales_online.ext_car_sales_online (
    id VARCHAR(250),
    date_of_transaction VARCHAR(250),
    employee_id VARCHAR(250),
    employee VARCHAR(250),
    customer_name_and_surname VARCHAR(250),
    customer_country VARCHAR(250),
    customer_state VARCHAR(250),
    customer_postal_code VARCHAR(250),
    customer_city VARCHAR(250),
    customer_street_name VARCHAR(250),
    customer_building_number VARCHAR(250),
    customer_telephone_number VARCHAR(250),
    customer_email VARCHAR(250),
    brand VARCHAR(250),
    car_model VARCHAR(250),
    car_year VARCHAR(250),
    car_price VARCHAR(250),
    reduction_percent VARCHAR(250),
    income VARCHAR(250),
    payment_method VARCHAR(250),
    sales_channel VARCHAR(250),
    car_dealer_name VARCHAR(250)
)
SERVER sa_car_sales_online
OPTIONS(
	filename 'C:\Program Files\PostgreSQL\17\data\csv_data\car_sales_online.csv',
	format 'csv',
	header 'true',
	delimiter ';',
	null '',
	escape '"'
);
 
 
CREATE TABLE sa_car_sales_online.src_car_sales_online (
    id VARCHAR(250),
    date_of_transaction VARCHAR(250),
    employee_id VARCHAR(250),
    employee VARCHAR(250),
    customer_name_and_surname VARCHAR(250),
    customer_country VARCHAR(250),
    customer_state VARCHAR(250),
    customer_postal_code VARCHAR(250),
    customer_city VARCHAR(250),
    customer_street_name VARCHAR(250),
    customer_building_number VARCHAR(250),
    customer_telephone_number VARCHAR(250),
    customer_email VARCHAR(250),
    brand VARCHAR(250),
    car_model VARCHAR(250),
    car_year VARCHAR(250),
    car_price VARCHAR(250),
    reduction_percent VARCHAR(250),
    income VARCHAR(250),
    payment_method VARCHAR(250),
    sales_channel VARCHAR(250),
    car_dealer_name VARCHAR(250)
);
 
INSERT INTO sa_car_sales_online.src_car_sales_online (
    id, date_of_transaction, employee_id, employee, customer_name_and_surname,
    customer_country, customer_state, customer_postal_code, customer_city,
    customer_street_name, customer_building_number, customer_telephone_number,
    customer_email, brand, car_model, car_year, car_price, reduction_percent,
    income, payment_method, sales_channel, car_dealer_name
)
SELECT
    id, date_of_transaction, employee_id, employee, customer_name_and_surname,
    customer_country, customer_state, customer_postal_code, customer_city,
    customer_street_name, customer_building_number, customer_telephone_number,
    customer_email, brand, car_model, car_year, car_price, reduction_percent,
    income, payment_method, sales_channel, car_dealer_name
FROM sa_car_sales_online.ext_car_sales_online;
 
 
-- SELECT * FROM sa_car_sales_online.ext_car_sales_online LIMIT 5;
-- SELECT * FROM sa_car_sales_online.src_car_sales_online; LIMIT 5;
-- SELECT COUNT(*) FROM sa_car_sales_online.src_car_sales_online;