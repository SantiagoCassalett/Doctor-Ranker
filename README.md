# doctor-ranker-master
Constructor for ranking doctors based on whether they chose to perform surgery on benign tumors.

#Background:
A project I worked on asked me to write a constructor that would traverse a database and return insights about doctor performance.
The constructor was expected to do the follow:
1) Output historical performance of each doctor in the database.
  a) This performance was based on a violation rate
  b) The violation rate considered the number of opportunities a doctor had to perform surgery on a benign tumor divided by the number of times they actually performed the surgery
  c) high performing doctors should have the lowest violation rates
  
The purpose of this constructor was to help determine which doctors were the highest performing to help guide information as to which doctor a patient should be referred to. 

#.py file
The .py file (ViolationRule.py) has four main functions:
1) dictionary_maker - this creates the dictionary of CPT and Diagnostic codes needed to search the larger medical database for benign tumors and surgery
2) search_DB - merges and searches an sqlite database of CPT and Diagnostic codes to find the needed records
3) cleaning_Data - finds only those patients with benign tumors and removes duplicates where there were multiple billing records/procedure codes for the same visit
4) rate_Construction - identifies the patients with surgery on benign tumors and the doctors associated, then calculates the violation rate for each doctor and outputs the rates as a .txt file

#Jupyter Notebook
The jupyter notebook illustrates the various steps of the ViolationRule.py script including the final output. 
