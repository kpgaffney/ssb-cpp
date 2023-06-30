import pandas as pd

df = pd.read_csv('results.csv')
df['Time (ms)'] = df['Time (s)'] * 1000

df_build = df[df['Operation'].str.startswith('Build')]
df_probe = df[df['Operation'] == 'Probe']
df_agg = df[df['Operation'] == 'Agg']
df_final = df[df['Operation'] == 'Finalize']

print('Build:')
print(df_build[['Query', 'Time (ms)']].groupby('Query').sum().to_string(index=False))

print('Probe:')
print(df_probe[['Query', 'Time (ms)']].groupby('Query').sum().to_string(index=False))

print('Agg:')
print(df_agg[['Query', 'Time (ms)']].groupby('Query').sum().to_string(index=False))

print('Final:')
print(df_final[['Query', 'Time (ms)']].groupby('Query').sum().to_string(index=False))
