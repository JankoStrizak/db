import cmd
import os
import sys
import mysql.connector
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="testing"
)
ourcursor = mydb.cursor()

def check_table(table_nm, limit_nm):
    tables = [
        "Crime",
        "Complaint",
        "CrimeLocation",
        "InternalClassification",
        "EndDateTime",
        "StartDateTime",
        "NYCCoords",
        "GlobalCoords",
        "InternalClassification",
        "PopByBoro",
        "NYCCoords",
        "GlobalCoords",
    ]

    for x in tables:
        if table_nm.casefold() == x.casefold():
            q = "SELECT * FROM " + x + " LIMIT " + limit_nm
            ourcursor.execute(q)
            # mydb.commit()
            # print(ourcursor.rowcount, "record(s) affected")
            for j in ourcursor:
                print(j)


class HelloWorld(cmd.Cmd):
    """Simple command processor example."""

    def do_EOF(self, line):
        return True

###############################################
##########read functions#####################
###############################################

    # getting a specific crime: attribute = OFNS_DESC from Complaint Table. Ex. Dangerous Weapons
    def do_TypeOfCrime(self, line):
        line = line.split(",")
        ofns_desc = line[0]

        q = 'SELECT * FROM Crime WHERE OFNS_DESC="' + ofns_desc + '"'
        ourcursor.execute(q)
        for j in ourcursor:
            print(j)

    # getting cardinality
    def do_NumTypeOfCrime(self, line):
        line = line.split(",")
        ofns_desc = line[0]

        q = 'SELECT COUNT(*) FROM Crime WHERE OFNS_DESC ="' + ofns_desc + '"'
        ourcursor.execute(q)
        for j in ourcursor:
            print(j)

    #gets start date of a cime
    def do_StartDateOfCrime(self, line): #done
        line = line.split(",")
        complaint_num = line[0]

        q = "SELECT CMPLNT_FR_DT,CMPLNT_FR_TM FROM StartDateTime WHERE CMPLNT_NUM=" + complaint_num
        ourcursor.execute(q)
        for x in ourcursor:
            print(x)

    #gets the borough in which a crime occured
    def do_BoroughOfCrime(self, line): #done
        line = line.split(",")
        complaint_num = line[0]

        q = "SELECT Boro_nm FROM Crimelocation inner join occursat USING(ADDR_PCT_CD,LOC_OF_OCCUR_DESC,PREM_TYP_DESC) WHERE CMPLNT_NUM=" + complaint_num
        ourcursor.execute(q)
        for x in ourcursor:
            print(x)

    # return all the complaints that are not given by a specific jursidiction
    # q = 'SELECT * FROM Complaint WHERE JURIS_DESC!="' + 'N.Y. POLICE DEPT'"
    def do_ComplaintsNotByJuris(self, line):
        line = line.split(",")
        juris_desc = line[0]
        q = 'SELECT * FROM Complaint WHERE JURIS_DESC!="' + juris_desc + '"'
        ourcursor.execute(q)
        for j in ourcursor:
            print(j)

    #returns all complaint in a jurisdiction
    def do_ComplaintsByJuris(self, line):
        line = line.split(",")
        juris_desc = line[0]
        q = 'SELECT * FROM Complaint WHERE JURIS_DESC="' + juris_desc + '"'
        ourcursor.execute(q)
        for j in ourcursor:
            print(j)

    # Getting Population By Borough
    def do_PopByBoroughs(self, line):
        line = line.split(",")
        borough = line[0]
        year = line[1]
        yr_param = "SELECT YR_" + str(line[1])
        # ex. select YR_1950 from PopByBoro where Borough = 'Bronx';
        q = yr_param + ' FROM PopByBoro WHERE Borough="' + borough + '"'
        ourcursor.execute(q)
        for j in ourcursor:
            print(j)
    
    # Getting Percent Population By Borough
    def do_PopByBoroughsInPercent(self, line):
        line = line.split(",")
        borough = line[0]
        yr_param = "SELECT " + str(line[1]) + "_BORO_SHARE"
        # ex. select 1950_BORO_SHARE from PopByBoro where Borough = 'Bronx'
        q = yr_param + ' FROM PopByBoro WHERE Borough="' + borough + '"'
        ourcursor.execute(q)
        for j in ourcursor:
            print(j)


###############################################
##########update functions#####################
###############################################

    #changes jurisdiction of a percinct
    def do_ChangeJurisdiction(self, line): 
        line = line.split(",")
        presinct = line[0]
        new_jur = line[1]

        q = "UPDATE Complaint  INNER JOIN occursat ON occursat.CMPLNT_NUM = Complaint.CMPLNT_NUM SET Complaint.juris_desc = \"" + new_jur + "\" WHERE ADDR_PCT_CD=" + presinct
        ourcursor.execute(q)
        mydb.commit()
        print(ourcursor.rowcount, "record(s) affected")

    #changes population of a borough
    def do_change_boro_pop(self, line): 
        line = line.split(",")
        boro = line[0]
        year = line[1]
        pop = line[2]
        percent = line[3]

        q = "UPDATE popbyboro SET YR_"+year+"=\""+pop+"\", "+year+"_BORO_SHARE=\""+percent+"\" WHERE Borough=\""+boro+"\""
        ourcursor.execute(q)
        mydb.commit()
        print(ourcursor.rowcount, "record(s) affected")

    #an old borough gets amalgomated with another borough
    def do_absorb_boro(self, line): 
        line = line.split(",")
        old_boro = line[0]
        absorbed_into = line[1]

        q= "UPDATE crimelocation SET BORO_NM=\""+absorbed_into+"\" WHERE BORO_NM=\""+old_boro+"\""
        ourcursor.execute(q)
        mydb.commit()
        print(ourcursor.rowcount, "record(s) affected")

    #changes the borough of a specific point
    def do_change_borough_boundary(self, line):
        line = line.split(",")
        x = line[0]
        y = line[1]
        boro = line[2]

        q = "UPDATE crimelocation SET crimelocation.boro_nm=\""+boro+ \
        "\" WHERE ADDR_PCT_CD=(select ADDR_PCT_CD FROM locationat inner join NYCcoords USING(X_COORD_CD, Y_COORD_CD) WHERE X_COORD_CD="\
        +x+" AND Y_COORD_CD="+y+ \
        ") AND LOC_OF_OCCUR_DESC=(select LOC_OF_OCCUR_DESC FROM locationat inner join NYCcoords USING(X_COORD_CD, Y_COORD_CD) WHERE X_COORD_CD="+\
        x+" AND Y_COORD_CD="+y+\
        ") AND PREM_TYP_DESC=(select PREM_TYP_DESC FROM locationat inner join NYCcoords USING(X_COORD_CD, Y_COORD_CD) WHERE X_COORD_CD="+\
        x+" AND Y_COORD_CD=" + y+")"

        ourcursor.execute(q)
        mydb.commit()
        print(ourcursor.rowcount, "record(s) affected")

    #changes the value of specific attributes given the complaint_num
    def do_ChangeAttribute(self, line): 
        line = line.split(",")
        table_name = line[0]
        attr = line[1]
        complaint_num = line[2]
        new_val = line[3]

        if table_name == "Complaint" or table_name == "complaint": 
            q = "UPDATE Complaint SET " + attr + "=\"" + \
                new_val + "\" WHERE CMPLNT_NUM=" + complaint_num
            ourcursor.execute(q)
            mydb.commit()
            print(ourcursor.rowcount, "record(s) affected")

        if table_name == "Crime" or table_name == "crime": 
            q = "UPDATE Crime INNER JOIN Has ON Crime.KY_CD = Has.KY_CD SET Crime." + \
                attr + "=\"" + new_val + "\" WHERE CMPLNT_NUM=" + complaint_num
            ourcursor.execute(q)
            mydb.commit()
            print(ourcursor.rowcount, "record(s) affected")
            
        if table_name == "InternalClassification" or table_name == "Internalclassification" or table_name == "internalclassification": #done
            q = "UPDATE internalclassification SET " + attr + "=\"" + new_val \
                + "\" WHERE PD_CD=(SELECT PD_CD FROM HAS WHERE CMPLNT_NUM=" + complaint_num +")"
            ourcursor.execute(q)
            mydb.commit()
            print(ourcursor.rowcount, "record(s) affected")

        if table_name == "StartDateTime" or table_name == "Startdatetime" or table_name == "startdatetime": 
            q = ("UPDATE StartDateTime SET " + attr + '="' + new_val + '" WHERE CMPLNT_NUM=' + complaint_num)
            ourcursor.execute(q)
            mydb.commit()
            print(ourcursor.rowcount, "record(s) affected")

        if table_name == "EndDateTime" or table_name == "Enddatetime" or table_name == "enddatetime": 
            q = ("UPDATE EndDateTime SET " + attr + '="' + new_val + '" WHERE CMPLNT_NUM=' + complaint_num)
            ourcursor.execute(q)
            mydb.commit()
            print(ourcursor.rowcount, "record(s) affected")

        if (table_name == "CrimeLocation" or table_name == "Crimelocation" or table_name == "crimelocation"):
            q = ("UPDATE occursAt INNER JOIN CrimeLocation USING(ADDR_PCT_CD, LOC_OF_OCCUR_DESC, PREM_TYP_DESC) SET " \
                + attr + '="' + new_val + '" WHERE CMPLNT_NUM=' + complaint_num)
            ourcursor.execute(q)
            mydb.commit()
            print(ourcursor.rowcount, "record(s) affected")


if __name__ == '__main__':
    HelloWorld().cmdloop()
