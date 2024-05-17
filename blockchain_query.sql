/*
  Database format:
  The query below runs on Flipside. With minor changes it can be adapted to any SQL Engine that tracks ERC20 Transfers, Smart Contract Creations and EVM blockchain event logs.

  Example parameters to track the 1inch airdrop recipients on Flipside (don't forget to include the quotes):
    - CHAIN: ethereum
    - AIRDROP_DISTRIBUTION_CONTRACT: '0xe295ad71242373c37c5fda7b57f26f9ea1088afe'
    - TOKEN_CONTRACT_ADDRESS: '0x111111111117dc0aa78b770fa6a738034120c302'
    - Comment out line 36 `AND TO_ADDRESS NOT IN ({{ADDRESSES_TO_EXCLUDE}})` since we're not excluding any recipient
    - FIRST_BLOCK_NUMBER: 11511392
*/

WITH AIRDROP_RECIPIENTS AS (
  --Extract the list of users that claimed a given airdrop along with the block_number the airdrop was claimed at
  SELECT DISTINCT 
    TO_ADDRESS as ADDRESS,
    BLOCK_NUMBER
  FROM {{CHAIN}}.core.ez_token_transfers --ERC20 Transfers table
  WHERE 
    --The sender of the ERC20 transfer is the airdrop distribution contract. Careful, in some cases, it might be minted from the null address
    FROM_ADDRESS IN (
      {{AIRDROP_DISTRIBUTION_CONTRACT}}
    )

    --Restrict on the airdropped token's contract address
    AND contract_address = {{TOKEN_CONTRACT_ADDRESS}}

    --Exclude addresses that received 0 tokens
    AND AMOUNT > 0
    
    --some additional filters are recommended
    --Ensure the contract called is the distributing contract
    AND origin_to_address = {{AIRDROP_DISTRIBUTION_CONTRACT}}

    --eventually add addresses to remove (typically the deployer address when the project recalls the unclaimed tokens post-airdrop)
    AND TO_ADDRESS NOT IN ({{ADDRESSES_TO_EXCLUDE}})

    --For some airdrops, we might want to restrict airdrop recipients to users that called the Airdrop contract
    AND to_address = origin_from_address

    --For Query optimization add a lower limit on blocks
    AND block_number > {{FIRST_BLOCK_NUMBER}}

)
  

SELECT 
  airdrop_recipient, 
  sent_to, 
  amount, 
  block_number
FROM (
  SELECT 
    ar.address as airdrop_recipient,
    tx.to_address as sent_to,
    amount,
    tx.block_number,
    tx.contract_address,
    tx.origin_to_address,
    ROW_NUMBER() OVER (PARTITION BY ar.address ORDER BY tx.block_number) as tx_order
  FROM {{CHAIN}}.core.ez_token_transfers tx
    JOIN 
      AIRDROP_RECIPIENTS ar 
        on ar.address = tx.from_address
        --We take all the transactions that happened AFTER claiming the airdrop
        AND tx.block_number >= ar.block_number 
      
  WHERE 
    --Restrict on the airdropped token's contract address
    tx.contract_address = {{TOKEN_CONTRACT_ADDRESS}}
)
WHERE 
  --We restrict to the first transaction sent after claiming the airdrop
  tx_order = 1 
  
  --To avoid issues with intents (limit orders filled by 3rd party market makers) and other edge cases, the contract called in the transaction must be the token's contract address
  AND origin_to_address = contract_address

  --Transactions sent to smart contracts are excluded...
  AND sent_to not in (
    SELECT DISTINCT ADDRESS 
    FROM {{CHAIN}}.core.dim_contracts -- On flipside this is {chain}.core.dim_contracts
    WHERE Address not in (
      --... unless this smart contract is a gnosis safe (only latest version)
      SELECT CONTRACT_ADDRESS
      FROM {{CHAIN}}.core.fact_event_logs -- On flipside this is 
      WHERE TOPICS[0] = '0x141df868a6331af528e38c83b7aa03edc19be66e37ae67f9285bf4f8e3c6a1a8' 
    )
  )
