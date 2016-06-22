# Clicks aggregation based on campaign name 
Retrieve all table names
Filter clicks table name .
Scan every clicks table and retrieve useful data (receivedDate, campaignId, partnerId, publisherId, .) 
Check whether aggregate table already exist or not,
                If true : retrieve click count of every item and update back with increase of 1.
                 If false : create aggregate table first and then insert/update data.
