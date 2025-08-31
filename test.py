import pandas as pd

# Creating a sample DataFrame
data = pd.DataFrame({
    'Name': ['Geek1', 'Geek2', 'Geek3', 'Geek4', 'Geek5'],
    'Age': [25, 30, 22, 35, 28],
    'Salary': [50000, 60000, 45000, 70000, 55000]
})

# Setting 'Name' column as the index for clarity
#data.set_index('Name', inplace=True)

# Displaying the original DataFrame
print("Original DataFrame:")
print(data)

# Extracting a single row by index
x = list(set(data.iloc[:, 0]))
print(x)

