import pandas as pd
import networkx as nx

NAME = '{protocol_slug}_{chain_slug}' #i.e. 1inch_eth

#Contains an actively maintained list of EOAs related to centralized entities: Bridges, CEX Hot Wallets, etc...
##In practice users don't send their airdrop to centralized addresses but in edge cases it happens.
CENTRALIZED_ADDRESSES = pd.read_csv('centralized_addresses.csv')['address'].tolist() 

#read the results of the blockchain_query.sql with the following columns: AIRDROP_RECIPIENTS, SENT_TO, AMOUNT, BLOCK_NUMBER
df = pd.read_csv('data.csv') 

#Load the transactions into a graph
G = nx.from_pandas_edgelist(
    df[
        #filter for transactions with a value above 0
        (df.AMOUNT > 0)
        &
        
        (~df['AIRDROP_RECIPIENT'].isin(CENTRALIZED_ADDRESSES))
        &
  
        (~df['SENT_TO'].isin(
          CENTRALIZED_ADDRESSES
          #we also exclude transactions where users burned their airdrop
          + [
            '0x000000000000000000000000000000000000dead',
            '0x0000000000000000000000000000000000000000'
          ]
        ))
    ],
    'AIRDROP_RECIPIENT',
    'SENT_TO',
    create_using = nx.Graph()
)

# Find strongly connected components (clusters) in the graph
clusters = sorted(nx.connected_components(G), key=len, reverse=True)

#Assign a cluster ID from 1 to n to each cluster
user_cluster_mapping = {}
for i, cluster in enumerate(clusters):
    for user in cluster:
        user_cluster_mapping[user] = i + 1

#export address-cluster mappins, the cluster label containing the slug + cluster ID
df_clusters = pd.DataFrame(
    [[k,f'{NAME}_{v}'] for k,v in user_cluster_mapping.items()],
    columns = ['address','cluster_id']
)

df_clusters.to_csv('results.csv')
