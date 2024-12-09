import cmd
import os
import sys
import mysql.connector

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="NewYork_Crime_Complaints",
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

    def do_get_records(self, line):  # GetRecords <table>,<limit>
        line = line.split(",")
        table_name = line[0]
        limit = line[1]
        check_table(table_name, limit)
        # example: get_records EndDateTime,34

    # getting a specific crime: attribute = OFNS_DESC from Complaint Table.
    def do_type_of_crime(self, line):  # type_of_crime <offense description>
        line = line.split(",")
        ofns_desc = line[0]

        q = (
            'SELECT * FROM Crime inner join has USING(KY_CD)  WHERE OFNS_DESC="'
            + ofns_desc
            + '"'
        )
        ourcursor.execute(q)
        for j in ourcursor:
            print(j)
        # example: type_of_crime Dangerous Weapons

    # getting cardinality
    def do_num_type_of_crime(self, line):  # num_type_of_crime <offense description>
        line = line.split(",")
        ofns_desc = line[0]

        q = (
            'SELECT COUNT(*) FROM Crime inner join has USING(KY_CD) WHERE OFNS_DESC ="'
            + ofns_desc
            + '"'
        )
        ourcursor.execute(q)
        for j in ourcursor:
            print(j)
        # example: num_type_of_crime ASSAULT 3 & RELATED OFFENSES

    # gets start date of a cime
    def do_start_date_of_crime(self, line):  # start_date_of_crime <complaint_num>
        line = line.split(",")
        complaint_num = line[0]

        q = (
            "SELECT CMPLNT_FR_DT,CMPLNT_FR_TM FROM StartDateTime WHERE CMPLNT_NUM="
            + complaint_num
        )
        ourcursor.execute(q)
        for x in ourcursor:
            print(x)
        # example: start_date_of_crime 999962666

    # gets the borough in which a crime occured
    def do_borough_of_crime(self, line):  # borough_of_crime <complaint_num>
        line = line.split(",")
        complaint_num = line[0]

        q = (
            "SELECT Boro_nm FROM Crimelocation inner join occursat USING(ADDR_PCT_CD,LOC_OF_OCCUR_DESC,PREM_TYP_DESC) WHERE CMPLNT_NUM="
            + complaint_num
        )
        ourcursor.execute(q)
        for x in ourcursor:
            print(x)
        # example: borough_of_crime 999962666

    # return all the complaints that are not given by a specific jursidiction
    # q = 'SELECT * FROM Complaint WHERE JURIS_DESC!="' + 'N.Y. POLICE DEPT'"
    def do_complaints_not_by_juris(
        self, line
    ):  # complaints_not_by_juris <jurisdiction>
        line = line.split(",")
        juris_desc = line[0]
        q = 'SELECT * FROM Complaint WHERE JURIS_DESC!="' + juris_desc + '"'
        ourcursor.execute(q)
        for j in ourcursor:
            print(j)
        # example: complaints_not_by_juris N.Y. POLICE DEPT

    # returns all complaint in a jurisdiction
    def do_complaints_by_juris(self, line):  # complaints_by_juris <jurisdiction>
        line = line.split(",")
        juris_desc = line[0]
        q = 'SELECT * FROM Complaint WHERE JURIS_DESC="' + juris_desc + '"'
        ourcursor.execute(q)
        for j in ourcursor:
            print(j)
        # example: complaints_by_juris PORT AUTHORITY

    # Getting Population By Borough
    def do_pop_by_boroughs(self, line):  # pop_by_boroughs <borough>,<year>
        line = line.split(",")
        borough = line[0]
        year = line[1]
        yr_param = "SELECT YR_" + str(line[1])
        # ex. select YR_1950 from PopByBoro where Borough = 'Bronx';
        q = yr_param + ' FROM PopByBoro WHERE Borough="' + borough + '"'
        ourcursor.execute(q)
        for j in ourcursor:
            print(j)
        # example: pop_by_boroughs staten island,2040

    # Getting Percent Population By Borough
    def do_pop_by_boroughs_in_percent(
        self, line
    ):  # pop_by_boroughs_in_percent <borough>,<year>
        line = line.split(",")
        borough = line[0]
        yr_param = "SELECT " + str(line[1]) + "_BORO_SHARE"
        # ex. select 1950_BORO_SHARE from PopByBoro where Borough = 'Bronx'
        q = yr_param + ' FROM PopByBoro WHERE Borough="' + borough + '"'
        ourcursor.execute(q)
        for j in ourcursor:
            print(j)
        # example: pop_by_boroughs_in_percent queens,1980

    ###############################################
    ##########update functions#####################
    ###############################################

    # changes jurisdiction of a percinct
    # ex. change_jurisdiction 28, N.Y. HOUSING POLICE -> 4311 record(s) affected
    def do_change_jurisdiction(self, line):
        line = line.split(",")
        presinct = line[0]
        new_jur = line[1]

        q = (
            'UPDATE Complaint  INNER JOIN occursat ON occursat.CMPLNT_NUM = Complaint.CMPLNT_NUM SET Complaint.juris_desc = "'
            + new_jur
            + '" WHERE ADDR_PCT_CD='
            + presinct
        )
        ourcursor.execute(q)
        mydb.commit()
        print(ourcursor.rowcount, "record(s) affected")

    # changes population of a borough
    # ex. change_boro_pop brooklyn,1950, 500000,20??
    def do_change_boro_pop(self, line):
        line = line.split(",")
        boro = line[0]
        year = line[1]
        pop = line[2]
        percent = line[3]

        q = (
            "UPDATE popbyboro SET YR_"
            + year
            + '="'
            + pop
            + '", '
            + year
            + '_BORO_SHARE="'
            + percent
            + '" WHERE Borough="'
            + boro
            + '"'
        )
        ourcursor.execute(q)
        mydb.commit()
        print(ourcursor.rowcount, "record(s) affected")

    # an old borough gets amalgomated with another borough
    # ex. absorb_boro Bronx,Queens
    def do_absorb_boro(self, line):
        line = line.split(",")
        old_boro = line[0]
        absorbed_into = line[1]

        q = (
            'UPDATE crimelocation SET BORO_NM="'
            + absorbed_into
            + '" WHERE BORO_NM="'
            + old_boro
            + '"'
        )
        ourcursor.execute(q)
        mydb.commit()
        print(ourcursor.rowcount, "record(s) affected")

    # changes the borough of a specific point
    # ex. change_borough_boundary 1007314,241257,BRONX
    def do_change_borough_boundary(self, line):
        line = line.split(",")
        x = line[0]
        y = line[1]
        boro = line[2]

        q = (
            'UPDATE crimelocation INNER JOIN locationat USING(ADDR_PCT_CD, LOC_OF_OCCUR_DESC, PREM_TYP_DESC) SET crimelocation.boro_nm="'
            + boro
            + '" WHERE locationat.X_COORD_CD='
            + x
            + " AND locationat.Y_COORD_CD="
            + y
        )

        ourcursor.execute(q)
        mydb.commit()
        print(ourcursor.rowcount, "record(s) affected")

    # changes the value of specific attributes given the complaint_num
    # the old value for Complaint # 100000659 has a JURIS_DESC  = N.Y. POLICE DEPT
    # ex. change_attribute Complaint,JURIS_DESC,100000659,N.Y. HOUSING POLICE

    # ex. change_attribute Crime,KY_CD,338440707,351
    # this command will not work becuase we can't change KY_CD

    # Old value is Felony:
    # ex. change_attribute Crime,OFNS_DESC,850629480,FRAUDS

    def do_change_attribute(self, line):
        line = line.split(",")
        table_name = line[0]
        attr = line[1]
        complaint_num = line[2]
        new_val = line[3]

        if table_name == "Complaint" or table_name == "complaint":
            if attr == "CMPLNT_NUM":
                print("Can't change CMPLNT_NUM")
            else:
                q = (
                    "UPDATE Complaint SET "
                    + attr
                    + '="'
                    + new_val
                    + '" WHERE CMPLNT_NUM='
                    + complaint_num
                )
                ourcursor.execute(q)
                mydb.commit()
                print(ourcursor.rowcount, "record(s) affected")

        if table_name == "Crime" or table_name == "crime":
            if attr == "KY_CD":
                print("Can't change KY_CD")
            else:
                q = (
                    "UPDATE Crime INNER JOIN Has ON Crime.KY_CD = Has.KY_CD SET Crime."
                    + attr
                    + '="'
                    + new_val
                    + '" WHERE CMPLNT_NUM='
                    + complaint_num
                )
                ourcursor.execute(q)
                mydb.commit()
                print(ourcursor.rowcount, "record(s) affected")

        if (
            table_name == "InternalClassification"
            or table_name == "Internalclassification"
            or table_name == "internalclassification"
        ):
            if attr == "PD_CD":
                q = (
                    "UPDATE internalclassification SET "
                    + attr
                    + "="
                    + new_val
                    + " WHERE PD_CD=(SELECT PD_CD FROM HAS WHERE CMPLNT_NUM="
                    + complaint_num
                    + ")"
                )
            else:
                q = (
                    "UPDATE internalclassification SET "
                    + attr
                    + '="'
                    + new_val
                    + '" WHERE PD_CD=(SELECT PD_CD FROM HAS WHERE CMPLNT_NUM='
                    + complaint_num
                    + ")"
                )
            ourcursor.execute(q)
            mydb.commit()
            print(ourcursor.rowcount, "record(s) affected")

        if (
            table_name == "StartDateTime"
            or table_name == "Startdatetime"
            or table_name == "startdatetime"
        ):
            q = (
                "UPDATE StartDateTime SET "
                + attr
                + '="'
                + new_val
                + '" WHERE CMPLNT_NUM='
                + complaint_num
            )
            ourcursor.execute(q)
            mydb.commit()
            print(ourcursor.rowcount, "record(s) affected")

        if (
            table_name == "EndDateTime"
            or table_name == "Enddatetime"
            or table_name == "enddatetime"
        ):
            q = (
                "UPDATE EndDateTime SET "
                + attr
                + '="'
                + new_val
                + '" WHERE CMPLNT_NUM='
                + complaint_num
            )
            ourcursor.execute(q)
            mydb.commit()
            print(ourcursor.rowcount, "record(s) affected")

        if (
            table_name == "CrimeLocation"
            or table_name == "Crimelocation"
            or table_name == "crimelocation"
        ):
            if attr == "ADDR_PCT_CD":
                q = (
                    "UPDATE occursAt INNER JOIN CrimeLocation USING(ADDR_PCT_CD, LOC_OF_OCCUR_DESC, PREM_TYP_DESC) SET "
                    + attr
                    + "="
                    + new_val
                    + " WHERE CMPLNT_NUM="
                    + complaint_num
                )
            else:
                q = (
                    "UPDATE occursAt INNER JOIN CrimeLocation USING(ADDR_PCT_CD, LOC_OF_OCCUR_DESC, PREM_TYP_DESC) SET "
                    + attr
                    + '="'
                    + new_val
                    + '" WHERE CMPLNT_NUM='
                    + complaint_num
                )
            ourcursor.execute(q)
            mydb.commit()
            print(ourcursor.rowcount, "record(s) affected")


if __name__ == "__main__":
    HelloWorld().cmdloop()
