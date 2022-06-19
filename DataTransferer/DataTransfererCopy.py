import psycopg2
from psycopg2 import Error

def getPostgresData():
    try:
        mdmConnectionString = "host = 'localhost' port='5433' dbname = 'hadoopuser' user = 'hadoopuser' password = 'hadoop'"
        mdmConnection = psycopg2.connect(mdmConnectionString)
        mdmCursor = mdmConnection.cursor()
        mdmCursor.execute("SELECT * FROM hadoopuser.mdm.retention_time")
        retentionTime = mdmCursor.fetchall()
        return(retentionTime)

    except(Exception, Error) as error:
        print(error)
    
    finally:
        if mdmConnection:
            mdmCursor.close()
            mdmConnection.close()

def loadToCold(name, retTime):
    try:
        oddConnectionString = "host='localhost' port='5432' dbname='test' user='gpadmin' password='1045'"
        oddConnection = psycopg2.connect(oddConnectionString)
        oddCursor = oddConnection.cursor()
        oddCursor.execute (
            f"""INSERT INTO test.cold.{name}
                SELECT * FROM test.warm.{name}
                WHERE  date <  CURRENT_DATE - interval '{retTime} month';""")



    except(Exception, Error) as error:
        print(error)

    finally:
        if oddConnection:
            oddCursor.close()
            oddConnection.close()

def deleteFromWarm (name, retTime):
    try:
        oddConnectionString = "host='localhost' port='5432' dbname='test' user='gpadmin' password='1045'"
        oddConnection = psycopg2.connect(oddConnectionString)
        oddCursor = oddConnection.cursor()
        oddCursor.execute (
            f"""do $$
                BEGIN
                IF EXTRACT (MONTH FROM CURRENT_DATE) - {retTime} > 0 THEN
                DELETE FROM test.warm.{name}
                WHERE  date <  CURRENT_DATE - interval '{retTime} month';
                END IF;
                END$$;
                """)



    except(Exception, Error) as error:
        print(error)

    finally:
        if oddConnection:
            oddCursor.close()
            oddConnection.close()


def launch():
    for tuple in getPostgresData():
        name = tuple[1]
        retTime = tuple[2]
        loadToCold(name, retTime)
        deleteFromWarm(name,retTime)