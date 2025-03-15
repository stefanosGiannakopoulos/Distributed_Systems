import matplotlib.pyplot as plt

# Incorporate the baseline throughput into both lines at k=1
k_chain = [1, 3, 5]
throughput_chain = [182.12, 140.50, 106.04]

k_eventual = [1, 3, 5]
throughput_eventual = [186.41, 262.48, 415.90]

plt.figure()

# Plot Chain Replication
plt.plot(k_chain, throughput_chain, marker='o', label='Chain Replication')

# Plot Eventual Consistency
plt.plot(k_eventual, throughput_eventual, marker='o', label='Eventual Consistency')

# Diagram settings
plt.xlabel('k (Replication Factor)')
plt.ylabel('Throughput (queries/sec)')
plt.title('Throughput vs. Replication Factor')
plt.legend()
plt.grid(True)

plt.show()
