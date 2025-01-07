from strategies import momentum_strategy

# Generate portfolios for momentum strategy (example)
portfolios = momentum_strategy(type="monthly")

# Print the 10 decile portfolios for the last period (December 2024 portfolios)
print(portfolios[-1])