import matplotlib.pyplot as plt

# k values including k=1, 3, and 5
k_values = [1, 3, 5]

# Throughput values for each consistency model.
throughput_chain = [210.12, 139.14, 98.56]       # Chain Replication
throughput_eventual = [215.11, 169.82, 124.23]     # Eventual Consistency

plt.figure(figsize=(10, 6))
plt.plot(k_values, throughput_chain, marker='o', label='Chain Replication')
plt.plot(k_values, throughput_eventual, marker='o', label='Eventual Consistency')

plt.xlabel('k (Replication Factor)')
plt.ylabel('Throughput (inserts/sec)')
plt.title('Throughput vs. Replication Factor for Different Consistency Models')
plt.legend()
plt.grid(True)
plt.show()