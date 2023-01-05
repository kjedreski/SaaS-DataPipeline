# SaaS Data Integration
AWS and SaaS.  

The Goal's objective: Proof of Concept for designing a data pipeline between a SaaS system and delivering files to a S3 server. 

(Please note, this code isn't going to be optimized)

Marketo has something called 'program' which is essentially a marketing campaign.  Each of these marketing campaigns could have many people that belong to them.  The goal is to deliver the client people that have a relationship with these campaigns.

The Flow at a high level:
1. Hit several different Marketo REST API endpoints
2. Collect all the data points
3. Perform a mutation to get data into desirable format
4. Load data into a dictionary
5. using the Dictionary object -- write to a file in a AWS S3 bucket

The overall plan was to get this running in AWS so it is built with a lambda_function and boto






