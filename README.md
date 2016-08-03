# 1SDK Api in python

##Clicks aggregation based on campaign name 
1. Retrieve all table names for dynamodb account. <br />
2. Filter clicks table names. <br />
3. Scan every clicks table and retrieve useful data: <br /> 
   (receivedDate, campaignId, partnerId, publisherId, .) <br />
   Check whether aggregate table already exist or not, <br />
        If true : retrieve click count of every item and update back with increase of 1. <br />
        false : create aggregate table first and then insert/update data. <br />
        
##Clicks to install matching
###On production for every installs , the developer api is called with,
    ip-address, user agent, installation date, device id. 
    Here all the details are hardcoded.
1. check registration table for device id, 
     if there device is already registered, skip entire process.
2. else
     generate fingerprint with user agent and ip address
     fetch excat window match value in minutes from database, 
     scan clcks table with date range, 
        if there is an entry, found a matching click
        else go to next step
3. startig fuzzy match,
     fetch fuzzy window value,
     check clicks table with ip address and date range
     if match found,
        found fuzzy match
     else
        grouped as ORGANIC install
        
     
