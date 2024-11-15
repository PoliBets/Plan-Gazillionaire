import requests
import ast
import main
from datetime import datetime
from mysql.connector import Error
from polymarketapi import getpolymarketinfo

def update():
    #update polymarket info
    getpolymarketinfo()
    #update kalshi info here

    #goes through the database and if the expiration date is past, change to close
