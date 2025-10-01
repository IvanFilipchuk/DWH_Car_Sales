## Data Overview  

This project uses **two source datasets** (`CSV` files), representing offline and online car sales.  
Since the original business data is **private**, the repository contains only **sample data** , generated for demonstration purposes.  
The structure and naming conventions match the real system, but the content (customers, cars, dealers) is fully fictitious.

### Dataset 1: `car_sales_offline.csv`
Represents offline sales transactions.  
Main attributes:
- **Transaction details**: Id, Date, SalespersonId, Salesperson  
- **Customer details**: Name, Country, State, PostalCode, City, Address, Email, Telephone  
- **Car details**: CarMake, CarModel, CarYear, SalePrice, CommissionRate, CommissionEarned  
- **Payment & Channel**: PaymentMethod, SalesChannel  
- **Dealer details**: CarDealerName  

### Dataset 2: `car_sales_online.csv`
Represents online sales transactions.  
Main attributes:
- **Transaction details**: id, date_of_transaction, employee_id, employee  
- **Customer details**: customer_name_and_surname, country, state, postal_code, city, address, email, telephone  
- **Car details**: brand, car_model, car_year, car_price, reduction_percent, income  
- **Payment & Channel**: payment_method, Sales_channel  
- **Dealer details**: car_dealer_name  

### Differences
- Column names differ (e.g., `Id` vs `id`, `CarMake` vs `brand`).  
- Offline dataset includes **commission metrics** (CommissionRate, CommissionEarned).  
- Online dataset includes **discount & income metrics** (reduction_percent, income).  

Both datasets are loaded into the **Staging layer** before being transformed into the **3NF model** and then into the **Dimensional Model (Data Mart)**.
