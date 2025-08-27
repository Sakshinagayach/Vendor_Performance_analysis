#Python script for creating the final table adn cleaning that data with sql along with creatinf the final sales summary table as csv
import pandas as pd
import logging 
from ingestion_db import ingest_db
import sqlite3

logging.basicConfig(
    filename = "/Users/Sakshi.Nagayach/Downloads/LOGS/vendor_summary_logs",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s", 
    filemode ="a"
)
def vendor_sales_summary(conn):
    '''this function will creat a merged table form other tables to get the vendor summary'''
    vendor_sales_summary =pd.read_sql_query("""WITH FreightSummary AS (
       select
          VendorNumber,
          SUM(Freight)AS FreightCost
       from vendor_invoice
       group by VendorNumber
      ),
      PurchaseSummary AS (
          select
              p.VendorNumber,
              p.VendorName,
              p.Brand,
              p.Description,
              p.PurchasePrice,
              pp.Price AS ActualPrice,
              pp.Volume,
              SUM(p.Quantity) AS TotalPurchaseQuantity,
              SUM(P.Dollars)AS TotalPurchaseDollars
          from purchases p
          JOIN purchase_prices pp
          on p.Brand =pp.Brand
          Where p.PurchasePrice>0
          Group by  p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice,pp.Price ,pp.Volume
        ),
     SalesSummary AS (
         SELECT
           vendorNo,
           Brand,
           SUM(SalesQuantity) AS TotalSalesQuantity,
           SUM(salesdollars) as TotalSalesDollars,
           SUM(exciseTax) as TotalExciseTax,
           SUM(salesprice) as TotalSalesPrice
         FROM sales 
         Group by vendorNo,Brand
        )
    SELECT
       ps.VendorNumber,
       ps.VendorName,
       ps.Brand,
       ps.Description,
       ps.PurchasePrice,
       ps.ActualPrice,
       ps.Volume,
       ps.TotalPurchaseQuantity,
       ps.TotalPurchaseDollars,
       ss.TotalSalesQuantity,
       ss.TotalSalesDollars,
       ss.TotalExciseTax,
       ss.TotalSalesPrice,
       fs.FreightCost
    from PurchaseSummary ps
    LEFT JOIN SalesSummary ss
       ON ps.VendorNumber =ss.VendorNo
       AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs
       ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC """, conn)

    return vendor_sales_summary


def clean_data(vendor_sales_summary):
    vendor_sales_summary["Volume"]=vendor_sales_summary["Volume"].astype("float64")
    vendor_sales_summary.fillna(0, inplace=True)
    vendor_sales_summary["VendorName"]=vendor_sales_summary["VendorName"].str.strip()
    vendor_sales_summary["GrossProfit"]=vendor_sales_summary["TotalSalesDollars"]-vendor_sales_summary["TotalPurchaseDollars"]
    vendor_sales_summary["ProfitMargin"]=((vendor_sales_summary["GrossProfit"]/vendor_sales_summary["TotalSalesDollars"])*100)
    vendor_sales_summary["StockTurnover"]=vendor_sales_summary['TotalSalesQuantity']/vendor_sales_summary['TotalPurchaseQuantity']
    vendor_sales_summary["SalesPurchaseRatio"]=vendor_sales_summary["TotalSalesDollars"]/vendor_sales_summary["TotalPurchaseDollars"]

    return vendor_sales_summary
if __name__ == '__main__':
    #creating database connection
    conn = sqlite3.connect("inventory.db")

    logging.info("Creating Vendor Summary Table.....")
    summary_df= vendor_sales_summary(conn)
    logging.info(summary_df.head())


    logging.info("Cleaning data.....")
    clean_df= clean_data(summary_df)
    logging.info(clean_df.head())
    
    logging.info("Ingesting data.....")
    ingest_db(clean_df,"vendor_sales_summary",conn)
    logging.info('completed')


conn = sqlite3.connect('inventory.db')
df = pd.read_sql_query('select * from vendor_sales_summary', conn)
df.to_csv('summary_data.csv', index=False)
conn.close()
