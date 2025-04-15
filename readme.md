# Point of Sale Data Processing Project

## Development of batch data processing application
* The data for products lookup is in `masterdata\products.csv`
* Source data is in 3 zip files about 100k files
    * `batch_1_bills.zip`
    * `batch_2_bills.zip`
    * `batch_3_bad_bills.zip`

### Project objectives
* Create Billing and Billing Details Tables
* Develop DB routines to bulk load the billing data to Billing and Billing Details Tables
* Develop Python application to load billing data to Billing and Billing Details Tables
* Develop all exception scenarios for bad_bills

## Development of realtime data processing application
* Run the `RealtimeBillGenerator.py`
* The realtime bill data is generated as JSON files in the `bills` folder
* Develop a `Bill Processor` to 
   * Calculate the total bill price with product unit price lookup from `masterdata\products.csv`
   * Load Bill and Bill Details in DB tables
