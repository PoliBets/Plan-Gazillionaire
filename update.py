import requests
import ast
import main
from datetime import datetime
from mysql.connector import Error
from polymarketapi import getpolymarketinfo
from close_expired_events import close_past_events
from kalshiapi import get_kalshi_info
from arbitrage_calculator import update_arbitrage

def update():
    #update polymarket info
    getpolymarketinfo()
    #update kalshi info here
    get_kalshi_info()
    #goes through the database and if the expiration date is past, change to close
    close_past_events()
    #calculate arbitrage opportunities
    update_arbitrage()

update()