import pyodbc


def connect_db():
    # using Driver v.17 to support macOS 10.14 (Mojave) as per
    # https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/system-requirements?view=sql-server-ver15
    ODBC_STR = "Driver={ODBC Driver 17 for SQL Server};Server=tcp:sumrit-cmpt354.database.windows.net,1433;Database=VideoStore;Uid=sumrits;Pwd={pass$123};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    return pyodbc.connect(ODBC_STR)


if __name__ == '__main__':
    print(pyodbc.drivers())
    print(connect_db())
