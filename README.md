# Overview

This is a python script that uses the google sheets API to get the contents of a google sheets, update that data to a database, and then query the database for more data to be displayed in the google sheet.

This script provides visibility to users without requiring them to learn SQL to query the data they need.

[Software Demo Video](https://youtu.be/K16VzOWK38A)

# Relational Database

mySQL RDS database

The leads server has a leads table with several tables, including the leads table that has foreign keys to the employees table, the statuses table, and the providers table

# Development Environment

I used Django ORM to create and manage the database. Then I used python, icluding the google sheets API and the mysql.connector library to connect to the sheet and to the database.
