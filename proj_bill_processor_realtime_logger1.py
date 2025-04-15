import json
import os
import csv
import shutil
from datetime import datetime
import time
import logging

bills_folder = r"D:\nidhi\python\Billing\Bills"
invoices_folder = r"D:\nidhi\python\Billing\invoice_batch3"
products_file = r"D:\nidhi\python\Billing\masterdata\products.csv"
bad_bills_details = r"D:\nidhi\python\Billing\Bad_bills"
bad_bill_log=r"D:\nidhi\python\Billing\Bad_bills\billing_processing.log"


# Ensure necessary folders exist
os.makedirs(invoices_folder, exist_ok=True)
os.makedirs(bad_bills_details, exist_ok=True)

logging.basicConfig(
    filename=bad_bill_log,     
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def load_products():
    """Load product details from CSV into a dictionary."""
    products = {}
    try:
        with open(products_file, mode='r') as productfile:
            reader = csv.DictReader(productfile)
            for row in reader:
                product_id = int(row['product_id'])
                product_name = row['product_name']
                unit_price = float(row['unit_price'])
                products[product_id] = {
                    'name': product_name,
                    'price': unit_price
                }
    except FileNotFoundError:
        logging.error("Products file not found: %s", e)
        print("Products file not found.")
    except Exception as e:
        logging.error("Error loading products: %s", e)
        print(f"Error loading products: {e}")

    return products

def move_to_bad_bills(bill_path, bad_bills_details):
    """Move invalid bill JSON file to the bad bills folder."""
    try:
        shutil.move(bill_path, os.path.join(bad_bills_details, os.path.basename(bill_path)))
        print(f"{bill_path} moved to {bad_bills_details}")
    except Exception as e:
        logging.error("Failed to move %s: %s", bill_path, e)
        print(f"Failed to move {bill_path}: {e}")


def validate_bill(bill_dict, bill_path, products):
    """Validate bill format, date, and product details."""
    try:
        required_keys = ["BillID", "BillDate", "StoreID", "BillDetails"]
        for key in required_keys:
            if key not in bill_dict:
                raise KeyError(f"Missing required key: {key}")

        # Validate storeid
        store_id=int(bill_dict["StoreID"])
        
        if store_id < 0 or store_id > 4:
                raise  ValueError(f"Incorrect store id")
            
        # Validate date format
        datetime.strptime(bill_dict["BillDate"], "%m/%d/%Y %H:%M:%S")

        # Validate product IDs
        for item in bill_dict["BillDetails"]:
            if "ProductID" not in item:
                return False
            
            product_id = item["ProductID"]
            if product_id not in products:
                raise  ValueError(f"Incorrect product id")
                return False

        return True

    except (KeyError,ValueError) as e:
        logging.error("Error in %s: %s", bill_path, e)
        print(f"Error in {bill_path}: {e}")
        move_to_bad_bills(bill_path, bad_bills_details)
        return False
    

def load_bills():
    """Load all bill files from folder."""
    bill_files = []

    for f in os.listdir(bills_folder):
        full_path = os.path.join(bills_folder, f)
        #  # Get the full path
        if f.endswith(".json") and os.path.isfile(full_path):  # Check if it's a file
            bill_files.append(f)

    
    if not bill_files:
        print("No bill files found.")
        return []

    bills = []
    for bill_file in bill_files:
        bill_path = os.path.join(bills_folder, bill_file)
        try:
            with open(bill_path, "r") as file:
                bill_dict = json.load(file)
                bills.append((bill_dict, bill_path))
        except (FileNotFoundError, json.JSONDecodeError):
            move_to_bad_bills(bill_path, bad_bills_details)
            print(bill_path,"json not readable")

    return bills

def process_bill(bill_dict, products):
    """Generate invoice details from a valid bill."""
    total = 0
    invoice_details = []

    try:
        for item in bill_dict["BillDetails"]:
            product_id = item["ProductID"]
            quantity = item["Quantity"]

            unit_price = products[product_id]['price']
            product_name = products[product_id]['name']

            item_total = unit_price * quantity
            total += item_total

            invoice_details.append({
                'ProductID': product_id,
                'ProductName': product_name,
                'Quantity': quantity,
                'Unit Price': unit_price,
                'Amount': item_total
            })

        output_data = {
            'BillID': bill_dict['BillID'],
            'BillDate': bill_dict['BillDate'],
            'StoreID': bill_dict['StoreID'],
            'Total Amount': total,
            'Bill Details': invoice_details
        }

        return output_data
    except Exception as e:
        logging.error("Error processing bill: %s", e)
        print(f"Error processing bill: {str(e)}")
        return None

def save_invoice(invoice, bill_path):
    """Save invoice as a JSON file."""
    try:
        invoice_file = os.path.join(invoices_folder, f"{invoice['BillID']}.json")
        with open(invoice_file, "w") as f:
            json.dump(invoice, f, indent=4)
        print(f"Invoice saved: {invoice_file}")
        os.remove(bill_path)  # Remove original bill after successful processing
    except Exception as e:
        logging.error("Error saving invoice: %s", e)
        print(f"Error saving invoice: {e}")

def main():
    """Continuously process bills in real-time."""
    products = load_products()

    print(" Real-time bill processing started")
    

    while True:
        #bill_files = load_bills()
        bill_files = []

        for file in os.listdir(bills_folder):
            full_path = os.path.join(bills_folder, file)
                
            if file.endswith(".json") and os.path.isfile(full_path):  # Check if it's a file
                bill_files.append(file)
            

        if not bill_files:
            print("No new bills. Waiting for new files")
            continue

        for bill_file in bill_files:
            bill_path = os.path.join(bills_folder, bill_file)
            try:
                with open(bill_path, "r") as file:
                    bill_dict = json.load(file)
                
                if validate_bill(bill_dict, bill_path, products):
                    invoice = process_bill(bill_dict, products)
                    if invoice:
                        save_invoice(invoice, bill_path)
            except json.JSONDecodeError as e:
                logging.error("Error in %s: %s", bill_path, e)
                move_to_bad_bills(bill_path,bad_bills_details)
                print(f"Invalid JSON: {bill_path}")

        time.sleep(3)  # Short delay before next scan

if __name__ == "__main__":
    main()