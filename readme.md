📘 MongoDB ETL Pipeline (Python + Pandas)

This project demonstrates a simple ETL workflow using MongoDB as the data source and pandas for transformation.
It extracts customer and order data from a local MongoDB instance, applies cleaning and validation rules, merges both datasets, and prints the final combined output.

📂 Folder Structure
ETL Project/
│── .venv/                 # Virtual environment
│── src/
│   ├── extract.py         # MongoDB connection + data extraction
│   ├── transform.py       # Customer & order transformations + merge function
│   ├── main.py            # ETL pipeline orchestrator
│── readme.md              # Project documentation

🚀 What This Project Does
1️⃣ Extract

Uses PyMongo to fetch documents from MongoDB:

Connects to local MongoDB:
mongodb://localhost:27017/

Reads collections like:

customer

orders

Converts results into pandas DataFrames

Converts all _id (ObjectId) values to strings

2️⃣ Transform

Cleans and standardizes data:

customer_transform()

Drops _id column

Removes duplicates

Cleans & validates:

registration_date

email → generates fallback email if invalid

phone → fixes invalid phone numbers

Proper cases the customer name (title())

orders_transform()

Drops _id

Normalizes order_date

Converts order_amount to numeric

Removes invalid rows

3️⃣ Merge

merge_data(customer_df, orders_df, 'customer_id', 'left')
Performs a left join on customer_id.

4️⃣ Close

Closes MongoDB connection cleanly.

🛠 Requirements

Install dependencies:

pip install pandas pymongo

▶️ How to Run

From project root:

python src/main.py


Make sure MongoDB is running locally:

mongod

📘 File Details
src/extract.py

connect_mongodb() → Connects to localhost MongoDB

get_data(db_name, collection_name, query={}) → Returns MongoDB collection as DataFrame

close_mongodb() → Closes the client

src/transform.py

customer_transform(df) → Full cleaning + formatting

orders_transform(df) → Normalizes order data

merge_data(df1, df2, column, join) → Merges two DataFrames

src/main.py

Runs the ETL sequence:

Connect to MongoDB

Extract data

Transform customers

Transform orders

Merge

Print results

Close connection

🧪 Example Output

Running main.py prints:

Raw customer data

Raw orders data

Cleaned customer data

Cleaned orders data

Final merged dataset

All displayed as pandas DataFrames.

🔧 Future Enhancements (Optional)

Add a load step to store merged data back into MongoDB or MySQL

Add logging instead of prints

Add unit tests for transformation functions

Apply deeper validation for email & phone formats

Build a UI to view merged customer–order profiles

👤 Author

Sandeep Reddy
MongoDB • Python • ETL • Data Engineering