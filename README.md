# Clicks aggregation based on campaign name 
1. Retrieve all table names. <br />
2. Filter clicks table name. <br />
3. Scan every clicks table and retrieve useful data: <br /> 
   (receivedDate, campaignId, partnerId, publisherId, .) <br />
   Check whether aggregate table already exist or not, <br />
        If true : retrieve click count of every item and update back with increase of 1. <br />
        false : create aggregate table first and then insert/update data. <br />
