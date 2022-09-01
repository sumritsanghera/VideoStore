from fileinput import filename
import pyodbc
import datetime
from connect_db import connect_db


def loadRentalPlan(filename, conn):
    """
        Input:
            $filename: "RentalPlan.txt"
            $conn: you can get it by calling connect_db()
        Functionality:
            1. Create a table named "RentalPlan" in the "VideoStore" database on Azure
            2. Read data from "RentalPlan.txt" and insert them into "RentalPlan"
               * Columns are separated by '|'
               * You can use executemany() to insert multiple rows in bulk
    """
    # WRITE YOUR CODE HERE
    print('Connection Successful')
    print('Creating RentalPlan')

    conn.execute('''CREATE TABLE RentalPlan (
        pid INT,
        pname VARCHAR(50),
        monthly_fee FLOAT,
        max_movies INT,
        PRIMARY KEY (pid));
    ''')
   
    # Read File as per https://www.pythontutorial.net/python-basics/python-read-text-file/
    f = open(filename, "r")
    contents = f.readlines()
    
    for line in contents:
        values = line.split("|")
        pid = int(values[0])
        pname = values[1]
        monthly_fee = float(values[2])
        max_movies = int(values[3])

        # Write to Azure Table as per https://docs.microsoft.com/en-us/sql/machine-learning/data-exploration/python-dataframe-sql-server?view=sql-server-ver15
        conn.execute("INSERT INTO RentalPlan VALUES(?,?,?,?)", pid, pname, monthly_fee, max_movies)

    print('RentalPlan Complete')
    f.close()
    

def loadCustomer(filename, conn):
    """
        Input:
            $filename: "Customer.txt"
            $conn: you can get it by calling connect_db()
        Functionality:
            1. Create a table named "Customer" in the "VideoStore" database on Azure
            2. Read data from "Customer.txt" and insert them into "Customer".
               * Columns are separated by '|'
               * You can use executemany() to insert multiple rows in bulk
    """
    # WRITE YOUR CODE HERE
    print("Creating Customer")

    conn.execute('''CREATE TABLE Customer(
        cid INT,
        pid INT,
        username VARCHAR(50),
        password VARCHAR(50),
        PRIMARY KEY (cid),
        FOREIGN KEY (pid) REFERENCES RentalPlan(pid) 
        ON DELETE CASCADE);
    ''')

    f = open(filename, "r")
    contents = f.readlines()
    
    for line in contents:
        values = line.split("|")
        cid = int(values[0])
        pid = int(values[1])
        username = values[2]
        password = values[3]

        conn.execute("INSERT INTO Customer VALUES(?,?,?,?)", cid, pid, username, password)

    print("LoadCustomer Complete")
    f.close()


def loadMovie(filename, conn):
    """
        Input:
            $filename: "Movie.txt"
            $conn: you can get it by calling connect_db()
        Functionality:
            1. Create a table named "Movie" in the "VideoStore" database on Azure
            2. Read data from "Movie.txt" and insert them into "Movie".
               * Columns are separated by '|'
               * You can use executemany() to insert multiple rows in bulk
    """
    # WRITE YOUR CODE HERE
    print("Creating Movie")

    conn.execute('''CREATE TABLE Movie(
        mid INT,
        mname VARCHAR(50),
        year INT,
        PRIMARY KEY (mid));
    ''')

    f = open(filename, "r")
    contents = f.readlines()

    for line in contents:
        values = line.split("|")
        mid = int(values[0])
        mname = values[1]
        year = int(values[2])

        conn.execute("INSERT INTO Movie VALUES(?,?,?)", mid, mname, year)
    
    f.close()
    print("Movie Complete")

def loadRental(filename, conn):
    """
        Input:
            $filename: "Rental.txt"
            $conn: you can get it by calling connect_db()
        Functionality:
            1. Create a table named "Rental" in the VideoStore database on Azure
            2. Read data from "Rental.txt" and insert them into "Rental".
               * Columns are separated by '|'
               * You can use executemany() to insert multiple rows in bulk
    """
    # WRITE YOUR CODE HERE
    print("Creating loadRental")

    conn.execute('''CREATE TABLE Rental(
        cid INT,
        mid INT,
        date_and_time DATETIME,
        status VARCHAR(6),
        FOREIGN KEY (cid) REFERENCES Customer(cid) ON DELETE CASCADE,
        FOREIGN KEY (mid) REFERENCES Movie(mid) ON DELETE CASCADE);
    ''')

    f = open(filename, "r")
    contents = f.readlines()

    for line in contents:
        values = line.split("|")
        cid = int(values[0])
        mid = int(values[1])
        #Using strptime method as per https://www.programiz.com/python-programming/datetime/strptime
        #and strftime as per https://stackoverflow.com/questions/30112357/typeerror-descriptor-strftime-requires-a-datetime-date-object-but-received
        testeddate = datetime.datetime.strptime(values[2], "%Y-%m-%d %H:%M:%S")
        date_and_time = datetime.datetime.strftime(testeddate, "%Y-%m-%d %H:%M:%S")
        status = values[3].strip()

        conn.execute("INSERT INTO Rental VALUES(?,?,?,?)", cid, mid, date_and_time, status)

    f.close()
    
    print("All tables successful!")



def dropTables(conn):
    conn.execute("DROP TABLE IF EXISTS Rental")
    conn.execute("DROP TABLE IF EXISTS Customer")
    conn.execute("DROP TABLE IF EXISTS RentalPlan")
    conn.execute("DROP TABLE IF EXISTS Movie")



if __name__ == "__main__":
    conn = connect_db()

    dropTables(conn)

    loadRentalPlan("RentalPlan.txt", conn)
    loadCustomer("Customer.txt", conn)
    loadMovie("Movie.txt", conn)
    loadRental("Rental.txt", conn)


    conn.commit()
    conn.close()






