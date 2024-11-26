# globals.py

# Initialize arbitrage_sides_lookup
arbitrage_sides_lookup = {}

def add_to_arbitrage_sides_lookup(arb_id, bet_side_1, bet_side_2):
    arbitrage_sides_lookup[arb_id] = {
        "bet_side_1": bet_side_1,
        "bet_side_2": bet_side_2
    }
    print(f"Added to arbitrage_sides_lookup: arb_id={arb_id}, bet_side_1={bet_side_1}, bet_side_2={bet_side_2}")
    print(f"Current state of arbitrage_sides_lookup: {arbitrage_sides_lookup}")
