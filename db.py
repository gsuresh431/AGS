import sqlalchemy as db
from dateutil.relativedelta import relativedelta
import logging

def create_connection():
    '''
        create a database connection to sqllite
    '''
    return db.create_engine(f"sqlite:///./ATG.db")

def add_product(product):
    '''
    This is for product creation by adding record in the PRODUCT table
    '''    
    try:
        my_conn = create_connection()
        connection = my_conn.connect()
        metadata = db.MetaData()
        end_date = product.StartDate + relativedelta(months=product.DurationMonths)
        pre_tbl = db.Table('PRODUCT_RULE_ENGINE', metadata, autoload=True, autoload_with=my_conn)
        query = db.select([pre_tbl]).where(db.and_(pre_tbl.columns.PRODUCTNAME == product.ProductName))
        ResultProxy = connection.execute(query)
        ResultSet = ResultProxy.fetchall()

        email_date_before_expiration = end_date
        email_date_after_activation = None
        if ResultSet[0][1] is not None:
            email_date_after_activation = product.StartDate + relativedelta(days=int(ResultSet[0][1]))
        if ResultSet[0][2] is not None:
            email_date_before_expiration = end_date - relativedelta(days=int(ResultSet[0][2]))

        query = "INSERT INTO PRODUCT (CustomerId, ProductName, Domain, StartDate, DurationMonths, EndDate, EmailDateActivation, EmailDateExpiration) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        ResultProxy = connection.execute(query, (product.CustomerId, product.ProductName, product.Domain, product.StartDate, product.DurationMonths, end_date, email_date_after_activation, email_date_before_expiration))
        return {'Sucess':'Product {} added successfull'.format(product.ProductName)}
    except Exception as ex:
        logging.error(ex)
        return {'Error':'Error while adding the product. Please check the log'}
    finally:
        connection.close()

def delete_product(customerId, productName, domain):
    '''
        Delete the product
    '''
    try:
        my_conn = create_connection()
        connection = my_conn.connect()
        metadata = db.MetaData()
        product_tbl = db.Table('PRODUCT', metadata, autoload=True, autoload_with=my_conn)
        query = db.delete(product_tbl)
        query = query.where(db.and_(db.and_(product_tbl.columns.CustomerId == customerId, product_tbl.columns.ProductName == productName, product_tbl.columns.Domain == domain)))
        ResultProxy = connection.execute(query)
        return {'Sucess':'Product {} deleted successfully for domain {}'.format(productName, domain)}
    except Exception as ex:
        logging.error(ex)
        return {'Error':'Error while removing the product {}. Please check the log'.format(productName)}
    finally:
        connection.close()

def email_schedule():
    '''
        List the product schedules based on mails
    '''
    my_conn = create_connection()
    connection = my_conn.connect()
    metadata = db.MetaData()
    product_tbl = db.Table('PRODUCT', metadata, autoload=True, autoload_with=my_conn)
    query = db.select([product_tbl])
    ResultProxy = connection.execute(query)
    ResultSet = ResultProxy.fetchall()

    list_of_schedules = []
    for idx, i in enumerate(ResultSet):
        for j in (ResultSet[idx][6], ResultSet[idx][7]):
            if j:
                product_dict = {}
                product_dict['CustomerID'] = ResultSet[idx][0]
                product_dict['ProductName'] = ResultSet[idx][1]
                product_dict['Domain'] = ResultSet[idx][2]
                product_dict['EmailDate'] = str(j)
                list_of_schedules.append(product_dict)

    return sorted(list_of_schedules, key=lambda k: k['EmailDate'])