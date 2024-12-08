import matplotlib.pyplot as plt
import pandas as pd

# Data from the CSV
data = {
    "Week": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17],
    "Frequency": [7, 13, 11, 12, 11, 14, 11, 16, 7, 7, 10, 4, 3, 0, 7, 11, 2]
}

# Create a DataFrame
df = pd.DataFrame(data)

# Plotting
plt.figure(figsize=(12, 6)) 
plt.bar(df["Week"], df["Frequency"], color='skyblue', edgecolor='black')
plt.xlabel("Week", fontsize=18)  
plt.ylabel("Frequency", fontsize=18)
plt.xticks(df["Week"], fontsize=14)  
plt.yticks(fontsize=14)
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.show()
