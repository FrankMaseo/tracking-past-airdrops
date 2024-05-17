# tracking-past-airdrops

This repo outlines the basic data mining process to extract post-airdrop consolidation data. It's very common for airdrop farmers to consolidate the proceeds of an airdrop, once claimed, to a single address. Down the line it is often a single CEX deposit address. 

We track these post-airdrop movements only and log the clusters of wallets that have been caught consolidating the proceeds of an airdrop to an EOA, proving with 100% confidence that they belong to the same person. In the rare cases where the consolidated EOA is a centralized address operated by a bridge, a market maker or a CEX hot wallet (any EOA with bot-like behavior), we err on the side of accuracy and exclude the resulting cluster from our database.

1. The first part of the data mining process is detailed in the (blockchain_query.sql)[https://github.com/FrankMaseo/tracking-past-airdrops/blob/main/blockchain_query.sql] file where we collect the on-chain transactions after users claimed a given airdrop. This query is written to run on Flipside's SQL engine but can easily be adapted to other blockchain indexing services.
1. The second part of the process detailed in the (tag_addresses.py)[https://github.com/FrankMaseo/tracking-past-airdrops/blob/main/tag_addresses.py] is the filtering and clustering that leads to producing a mapping between a user address and a cluster ID

Down the line, we benchmark these collected mappings against LZ's user database to identify airdrop farming clusters.
