import psycopg2
from psycopg2 import Error

def launch():
    try:
        GPConnectionString = "host='localhost' port='5432' dbname='test' user='gpadmin' password='1045'"
        GPConnection = psycopg2.connect(GPConnectionString)
        GPCursor = GPConnection.cursor()
        GPCursor.execute (
        """INSERT INTO test.warm.request
        SELECT
            id,
            fullname as full_name,
            product,
            to_date(reqdate, 'DD-MM-YYYY') as date,
            EXTRACT (MONTH FROM to_date(reqdate, 'DD-MM-YYYY')) as month
            from
            test.public.requests;
            INSERT INTO test.warm.delivery
            SELECT * FROM test.warm.request
            WHERE id BETWEEN 10001 AND 20000;

            DELETE FROM test.warm.request
            WHERE id BETWEEN 10001 AND 20000;

            INSERT INTO test.warm.purchase
            SELECT * FROM test.warm.request
            WHERE id BETWEEN 20001 AND 30000;

            DELETE FROM test.warm.request
            WHERE id BETWEEN 20001 AND 30000;

            INSERT INTO test.warm.visit
            SELECT * FROM test.warm.request
            WHERE id BETWEEN 30001 AND 40000;

            DELETE FROM test.warm.request
            WHERE id BETWEEN 30001 AND 40000;

            INSERT INTO test.warm.consultation
            SELECT * FROM test.warm.request
            WHERE id BETWEEN 40001 AND 50000;

            DELETE FROM test.warm.request
            WHERE id BETWEEN 40001 AND 50000;""")



    except(Exception, Error) as error:
        print(error)

    finally:
        if GPConnection:
            GPCursor.close()
            GPConnection.commit()
            GPConnection.close()