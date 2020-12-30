import pandas as pd
from sqlalchemy import create_engine
import sys

df_orders = pd.read_csv('data/pbe-orders.csv')
df_returns = pd.read_csv('data/pbe-returns.csv')

def populate_data(usr, pswd, port, db_name):
    """
    Takes Postgres username, password, port, and database name to
    populate SQL database with CSV data. Assumes Postgres is installed
    and the empty database has already been created.
    """
    engine = create_engine(f'postgresql://{usr}:{pswd}@localhost:{port}/{db_name}')
    print('Engine Created')
    df_orders.to_sql('orders', engine)
    print(f'Orders data successfully loaded to {db_name}')
    df_returns.to_sql('returns', engine)
    print(f'Returns data successfully loaded to {db_name}')


if __name__ == '__main__':
    ps_usr = sys.argv[1]
    ps_pswd = sys.argv[2]
    ps_port = sys.argv[3]
    ps_db_name = sys.argv[4]
    populate_data(ps_usr, ps_pswd, ps_port, ps_db_name)
    print('All done!')

