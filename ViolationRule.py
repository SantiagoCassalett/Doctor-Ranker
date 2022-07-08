##############################################
#IMPORT

#Modules needed for ViolationRule.py
import sqlite3, sys, argparse
import pandas as pd
import numpy as np

########################
#Functions

def dictionary_maker(Oscopy, Resection, Benign, Malignant):
    """ Needed to Create the CPT Codes and the Diagnositic Codes"""
    CPT_Terms = {
    "Oscopy": [Oscopy],
    "Resection": [Resection]
    }

    poly_recog = {
    "Benign": [Benign],
    "Malignant": [Malignant]
    }
    return(CPT_Terms, poly_recog)

def search_DB(CPT_Terms):
    """The search DB function opens the sqlite database and searches through the database to find
    all of the CPT codes associated with a particular procedure. 
    Requires: CPT_Terms
    The steps involved:
    1) Merge together tables and search through the table for particular CPT codes
    2) Returns the rows with the procedures of interest with information on doctor_id, patient_id,
    # and the 'encounter_key'"""
    
    #### Preparing Data ####
    
    #Creating lists from the dictionaries - will be used to create placeholders
    # to avoid sql injection
    
    oscopy = CPT_Terms['Oscopy']
    surgery = CPT_Terms['Resection']
    
    #Placeholders
    placeholder_oscopy = "?" #To avoid sql injection
    placeholders_oscopy = ", ".join([placeholder_oscopy] * len(oscopy))
    
    placeholder_surgery = "?" #To avoid sql injection
    placeholders_surgery = ", ".join([placeholder_surgery] * len(surgery))
    
    #### Opens Database #####
    
    con = sqlite3.connect("claims.db")
  
    query = "SELECT * from medical_service_lines INNER JOIN medical_headers ON medical_headers.encounter_key\
        = medical_service_lines.encounter_key WHERE procedure IN (%s) OR procedure IN (%s)" %(placeholders_oscopy,
                                                                                             placeholders_surgery)

    con = sqlite3.connect("claims.db")
    msl = pd.read_sql_query(query, con = con, params = (*oscopy, *surgery))

    con.close()
    
    #Returns the result of the database search
    
    return(msl)

def cleaning_Data(db_data, poly_recog):
    """Cleans the data pulled from the database.
    Required:
    1) db_data
    2) poly_recog (dictionary with the diagnositic codes)

    First step is to find only those rows associated with a benign tumor
    Searches across the new dataframe based on the diagnostic codes associated with benign
    First pass is to grab the benign and the second is to remove those rows also associated with malignant"""
    
    MslBenignFirst = db_data[db_data.isin(poly_recog["Benign"]).any(axis = 1)]
    MslBenignOnly = MslBenignFirst[~MslBenignFirst.isin(poly_recog["Malignant"]).any(axis = 1)]

    #This makes sure that there are no duplicate columns
    MslBenignOnly = MslBenignOnly.loc[:,~MslBenignOnly.columns.duplicated()]
    
    #Cleans the dataframe so that there are no duplicates on encounter_key and procedures
    # This makes sure that each row is associated with one encounter and one procedure
    # There is overlap between encounter and procedure - this is situations where they potentially
    # had a colonoscopy and a surgery (it could also mean that they were coded several times for the same
    # procedure however)

    MslBenignOnly_Clean =  MslBenignOnly.drop_duplicates(subset = ["encounter_key", "procedure"])
    
    return(MslBenignOnly_Clean)

def rate_Construction(cleanedData, CPT_Terms, output = None):
    """ Now I go about constructing the rule: Dr involved with Colonoscopy should not
    refer for surgery if they the patient is benign - here I use encounter instead of patient
    because there can be multiple encounters per patient - you want to know what the doctor is doing
    per treatment rather than per person - an admitted weakness is this does not
    take into account the history of the patients - maybe they had malignant polyps in the past
    so it makes sense for them to have surgery on all of their polyps? """
    
    #I now search across the cleaned dataframe for the CPT codes again
    # to divide the dataset by the CPT codes: 1) for surgery and 2) for colonoscopy

    Msl_SurgeryOnly = cleanedData[cleanedData.procedure.isin(CPT_Terms["Resection"])]
    Msl_OscopyOnly = cleanedData[cleanedData.procedure.isin(CPT_Terms["Oscopy"])]

    #Again need to drop any duplicates - it is here that I account for multiple CPT codes for same encounter
    # and same procedure
    Msl_OscopyOnly_cleaned = Msl_OscopyOnly.drop_duplicates(subset = ["encounter_key", "patient_id"])
    Msl_SurgeryOnly_cleaned = Msl_SurgeryOnly.drop_duplicates(subset = ["encounter_key", "patient_id"])
    
    #Next step is to grab all of the doctors who performed a colonoscopy
    # and see how many of them also performed a surgery on a patient with a benign polyp

    OscopyDr = list(Msl_OscopyOnly_cleaned.doctor_id)
    
    #NUMERATOR CONSTRUCTION

    # I then check to see how many of the doctors involved in a colonoscopy also did surgery

    OscopyDrThatDidSurgery = Msl_SurgeryOnly_cleaned[Msl_SurgeryOnly_cleaned.doctor_id.isin(OscopyDr)]

    # If you group this new dataframe by doctor_id and count - you can see how many
    # doctors (612) were found to have done surgery - this is the numerator for the violation rate

    NumeratorCount = OscopyDrThatDidSurgery.groupby("doctor_id", as_index = False)["encounter_key"].count()

    # What you find is that there were 612 doctors involved with both surgery and a colonoscopy
    # These were doctors who did surgery on a patient with a benign polyp and have
    # been binned as "bad doctors"

    BadDoctors = OscopyDrThatDidSurgery.drop_duplicates(subset = ["doctor_id"])
    BadDoctors_ID = list(BadDoctors.doctor_id)

    #DENOMINATOR CONSTRUCTION

    # I then check to see how many times the doctors had the chance to not break the violate rate:

    #Using BadDoctors_ID that I grab from above - I search through the colonoscopy records

    ViolationRuleOpporunities = Msl_OscopyOnly_cleaned[Msl_OscopyOnly_cleaned.doctor_id.isin(BadDoctors_ID)]

    #We find that in total (for all doctors) - they had the opportunity to not violate the rule for 5567 encounters

    #If we groupby dr and count the number of encounters - we can construct the Denominator for our rate
    DenominatorCount = ViolationRuleOpporunities.groupby("doctor_id", as_index = False )["encounter_key"].count()
    
    #VIOLATION RATE CONSTRUCTION

    #With the above NumeratorCount and DenominatorCount - I can now construct the violation rate
    RateConstructing = pd.merge(NumeratorCount, DenominatorCount, on = "doctor_id")
    RateConstructing = RateConstructing.rename(columns = {"doctor_id" : "Doctor", "encounter_key_x" : "Numerator",\
                       "encounter_key_y" : "Denominator" })

    RateConstructing["ViolationRate"] = RateConstructing["Numerator"]/RateConstructing["Denominator"]
    
    #VIOLATION RATE FOR GOOD DOCTORS

    #Need to do similar with the good doctors as well - those that did not do surgery on 
    # patients with a benign polyp

    #The number of doctors can be found by using the "Bad" Doctors ID and doing a 'not' search
    # on the Colonoscopy records

    OscopyDoctorNoSurgery = Msl_OscopyOnly_cleaned[~Msl_OscopyOnly_cleaned.doctor_id.isin(BadDoctors_ID)]

    #If if you look at the "doctor_id" you can see the number of "Good" doctors - or those who do not break the rule
    # This number is 613

    GoodDr = list(OscopyDoctorNoSurgery.doctor_id.unique())
    
    #Below also service as the violation rule opporutnties - the number of opporunities the good dr had to 
    # violate the rule
    VROppo_GoodDr = Msl_OscopyOnly_cleaned[~Msl_OscopyOnly_cleaned.doctor_id.isin(BadDoctors_ID)]\
        .groupby("doctor_id", as_index = False)["encounter_key"].count()

    #I am now inserting 0s into the Numerator since they did not violate the rule
    VROppo_GoodDr.insert(1, 'Numerator', 0)

    #Renaming the columns and then creating the ViolationRate
    VROppo_GoodDr = VROppo_GoodDr.rename(columns = {"doctor_id" : "Doctor", 
                       "encounter_key" : "Denominator" })
    VROppo_GoodDr["ViolationRate"] = VROppo_GoodDr["Numerator"]/VROppo_GoodDr["Denominator"]

    # BINDING TOGETHER THE FULL DR VIOLATION RATE

    #Now I just need to append these values together
    ViolationRateFull = RateConstructing.append(VROppo_GoodDr)
    
    ViolationCSV = ViolationRateFull.to_csv(header = True, index = False, sep = ',')
    
    #I now have the full violation rate
    return(ViolationCSV)

    
########## Main Function ###########

def main():
    """ The main function that calls the search_DB function"""
    
    if args.Oscopy == None:
        CPT_Terms = {
            "Oscopy": ["45378", "45380", "45381", "45382", "45383", "45384", "45385",
              "45388"],
            "Resection": ["44110", "44146", "44150", "44151", "44152", "44153", "44154", "44155", "44156",
                 "44157", "44158", "44159", "44160", "44204", "44205", "44206", "44207", "44208", "44209",
                 "44210", "44211", "44212"]
        }
        poly_recog = {
            "Benign": ["211.3", "211.4"],
            "Malignant": ['152.0', '152.1', '152.2', '152.3', '152.4', 
                                       '152.5', '152.6', '152.7', '152.8', '152.9']
        }
    
        msl = search_DB(CPT_Terms)
    
        cleanedData = cleaning_Data(msl, poly_recog)
    
        ViolationRate = rate_Construction(cleanedData, CPT_Terms, args.output)
    
    else:
        Codes = dictionary_maker(Oscopy, Resection, Benign, Malignant)
        
        msl = search_DB(Codes)
    
        cleanedData = cleaning_Data(msl, Codes)
    
        ViolationRate = rate_Construction(cleanedData, CPT_Terms, args.output)
    
    if args.output == None:
        print(ViolationRate)
    
    else:
        args.output.write("%s" %ViolationRate)
        print("ViolationRate complete! Check your file!\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-Oscopy", "--OscopyList", dest = "Oscopy", nargs = '+', required = False)
    parser.add_argument("-Resection", "--ResectionList", dest = 'Resection', nargs = '+', required = False)
    parser.add_argument("-Benign", "--BenignList", dest = 'Benign', nargs = '+', required = False)
    parser.add_argument("-Malignant", "--MalList", dest = 'Malignant', nargs = '+', required = False)
    parser.add_argument("-Diag", "--DiagList", dest = "Diagnostic_codes", nargs = '+', required = False)
    parser.add_argument("-o", "--output", dest= "output", type = argparse.FileType('w'), required = False)
    args = parser.parse_args()
    main()
    

        
    