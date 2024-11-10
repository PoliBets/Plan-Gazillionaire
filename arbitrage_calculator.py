def calculate_cross_market_arbitrage(price_yes_market1, price_no_market2, price_no_market1, price_yes_market2):
    """
    Calculate arbitrage opportunity across two binary markets.

    Parameters:
    - price_yes_market1 (float): Price of the "Yes" contract in the first market in cents.
    - price_no_market2 (float): Price of the "No" contract in the second market in cents.
    - price_no_market1 (float): Price of the "No" contract in the first market in cents.
    - price_yes_market2 (float): Price of the "Yes" contract in the second market in cents.

    Returns:
    - dict: Information on whether there's an arbitrage opportunity, and if so, the profit per contract for each scenario.
    """
    # Scenario 1: "Yes" on Market 1, "No" on Market 2
    total_price_scenario_1 = price_yes_market1 + price_no_market2
    if total_price_scenario_1 < 100:
        profit_scenario_1 = 100 - total_price_scenario_1
    else:
        profit_scenario_1 = 0

    # Scenario 2: "No" on Market 1, "Yes" on Market 2
    total_price_scenario_2 = price_no_market1 + price_yes_market2
    if total_price_scenario_2 < 100:
        profit_scenario_2 = 100 - total_price_scenario_2
    else:
        profit_scenario_2 = 0

    # Determine if there's an arbitrage opportunity in either scenario
    if profit_scenario_1 > 0 or profit_scenario_2 > 0:
        return {
            "arbitrage": True,
            "scenario_1": {
                "bet_on": ("Yes on Market 1", "No on Market 2"),
                "total_investment": total_price_scenario_1,
                "profit_per_contract": profit_scenario_1
            },
            "scenario_2": {
                "bet_on": ("No on Market 1", "Yes on Market 2"),
                "total_investment": total_price_scenario_2,
                "profit_per_contract": profit_scenario_2
            }
        }
    else:
        return {
            "arbitrage": False,
            "message": "No arbitrage opportunity available in either scenario.",
            "scenario_1": {
                "bet_on": ("Yes on Market 1", "No on Market 2"),
                "total_investment": total_price_scenario_1,
                "profit_per_contract": profit_scenario_1
            },
            "scenario_2": {
                "bet_on": ("No on Market 1", "Yes on Market 2"),
                "total_investment": total_price_scenario_2,
                "profit_per_contract": profit_scenario_2
            }
        }

# Example usage:
price_yes_market1 = 72  # Price of the "Yes" contract in Market 1 in cents
price_no_market2 = 25   # Price of the "No" contract in Market 2 in cents
price_no_market1 = 35   # Price of the "No" contract in Market 1 in cents
price_yes_market2 = 30  # Price of the "Yes" contract in Market 2 in cents

result = calculate_cross_market_arbitrage(price_yes_market1, price_no_market2, price_no_market1, price_yes_market2)

# Display the result
if result["arbitrage"]:
    print("Arbitrage opportunity found!")
    if result["scenario_1"]["profit_per_contract"] > 0:
        print("Scenario 1: Bet on Yes in Market 1 and No in Market 2")
        print(f"  Total Investment: {result['scenario_1']['total_investment']} cents")
        print(f"  Guaranteed Profit per Contract: {result['scenario_1']['profit_per_contract']} cents")
    if result["scenario_2"]["profit_per_contract"] > 0:
        print("Scenario 2: Bet on No in Market 1 and Yes in Market 2")
        print(f"  Total Investment: {result['scenario_2']['total_investment']} cents")
        print(f"  Guaranteed Profit per Contract: {result['scenario_2']['profit_per_contract']} cents")
else:
    print(result["message"])


"""
*** Arbitrage Logic ***

- Objective: Find arbitrage opportunities by betting on opposite outcomes across two binary markets.
- Condition: An arbitrage exists if the combined prices of opposite bets across two markets are less than 100 cents.
- Two Scenarios:
    1. Scenario 1: Bet "Yes" in Market 1 and "No" in Market 2.
        + Calculate combined price: price_yes_market1 + price_no_market2.
        + Profit per contract if combined price < 100: 100 - combined price.
    2. Scenario 2: Bet "No" in Market 1 and "Yes" in Market 2.
        + Calculate combined price: price_no_market1 + price_yes_market2.
        + Profit per contract if combined price < 100: 100 - combined price.
- Result: If either scenario yields a combined price < 100, there is a guaranteed profit equal to the difference from 100.

"""