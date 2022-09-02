# -------------------------------------------------------------------------------
# Name:        Fields Sorting
# Purpose:     intern
#
# Author:      rnicolescu
#
# Created:     29/08/2022
# Copyright:   (c) rnicolescu 2022
# Licence:     <your license here>
# -------------------------------------------------------------------------------

print 'Loading arcpy'
from arcpy import env
import arcpy
import os, sys, shutil, time

print 'Arcpy loaded!'

start = time.time()

class Sort(object):
    print "Reorders, in ascending or descending order, records in a feature class or table based on one or multiple fields. The reordered result is written to a new dataset"
    print "Depending on how much data you have stored, the process could take longer or not."
    print "This script firstly sorts the fields inside the shapefiles and secondly sorts the data inside field OBJECTID*"

    TEMP_FOLDER = 'backup_gdb'
    CWD = os.getcwd()
    GDB_FILES = []


    def __init__(self, path):
        self.path = path

    def inputGDB(self):
        # Check if the gdb has correct extension
        # If not, exit

        gdb_path = ""
        if self.path.endswith(".gdb"):
            gdb_path += self.path
            print "Path entered is valid!"

        else:
            print("Please add a valid gdb path")
            sys.exit()

        return r"{}".format(gdb_path)

    def backupGDB(self, gdb):
        # Copy the gdb to the backup folder
        env.overwriteOutput = True

        print "Gdb copied for backup"
        if not os.path.exists(os.path.join(Sort.CWD, Sort.TEMP_FOLDER)):
            shutil.copytree(src = gdb,
                            dst=os.path.join(Sort.CWD, Sort.TEMP_FOLDER))

        os.rename(os.path.join(Sort.CWD, Sort.TEMP_FOLDER),
                  os.path.join(Sort.CWD, Sort.TEMP_FOLDER) + ".gdb")



    def tempFiles(self):
        # Loop through all the files inside the gdb and search only thos with value inside
        # Crete temporary files from gdb with 'temp__' suffix
        # Create sorted list of fields

        env.workspace = self.path


        for fc in arcpy.ListFeatureClasses():
            # Iterate through all the features to see if is empty or not. If is empty, pass.
            # Will work only with the ones who have data

            i = 0
            fc_path = os.path.join(env.workspace, fc)
            with arcpy.da.SearchCursor(fc, ['Shape']) as cursor:
                for row in cursor:
                    i += 1
                    break
            if i == 0:
                continue
            else:
                print "Processing --> {}".format(fc)
                desc = arcpy.Describe(fc)
                tmpFCS = "temp__" + fc
                existing_fields = desc.Fields
                all_field_names = dict(zip([f.name for f in existing_fields], existing_fields))
                existing_field_names = [field.name for field in existing_fields if not field.required]
                field_order = sorted(existing_field_names)



                arcpy.CreateFeatureclass_management(out_path=os.path.join(env.workspace),
                                                    out_name=tmpFCS,
                                                    geometry_type=desc.shapeType,
                                                    spatial_reference=desc.spatialReference,)

                for fldName in field_order:
                    fld = all_field_names[fldName]
                    arcpy.AddField_management(tmpFCS, fld.name, fld.type, fld.precision, fld.scale, fld.length, \
                                              fld.aliasName, fld.isNullable, fld.required, fld.domain)


    def updateTMP_Fields(self):
        # Read all the rows from the input fc
        # Update the TMP files with rows from the input FC
        # Using InsertCursor and UpdateCursor
        # Pass the features with no values
        # Work only with the features with values inside

        env.workspace = self.path

        actual_fc = []
        temp_fc = []

        for fc in arcpy.ListFeatureClasses():
            i = 0
            fc_path = os.path.join(env.workspace , fc)
            with arcpy.da.SearchCursor(fc, ['Shape']) as cursor:
                for row in cursor:
                    i += 1
            if i == 0:
                continue
            else:
                actual_fc.append(fc)

        for fc in arcpy.ListFeatureClasses():
            if fc.startswith("temp__"):
                temp_fc.append(fc)

        for fc1 in actual_fc:
            for fc2 in temp_fc:
                if fc1 == fc2[6:]:
                    print "Joining {} with {}...".format(fc1, fc2)
                    arcpy.SpatialJoin_analysis(target_features=fc1,
                                               join_features=fc2,
                                               out_feature_class=os.path.join(env.workspace, "new__" + fc1),
                                               join_operation='JOIN_ONE_TO_ONE',
                                               join_type='KEEP_ALL')

        for fc in arcpy.ListFeatureClasses():
            if fc.startswith("new__"):
                Sort.GDB_FILES.append(fc)



    def sort_FCS(self, gdb_files):
        # Select only the features wich start with 'new__'
        # Sort all the field attributes
        # Save to new feature class
        env.workspace = self.path

        print "Deleting extra fields added. Please wait..."
        for fc in gdb_files:
            print "Deleting extra fields from {}".format(fc)
            for field in arcpy.ListFields(fc):
                if "_1" in field.name:
                    arcpy.DeleteField_management(fc, field.name)
                if field.name == "Join_Count" or field.name == "TARGET_FID":
                    arcpy.DeleteField_management(fc, field.name)

        for feature_class in gdb_files:
            for field in arcpy.ListFields(feature_class):
                print "Sorting field {} from {}".format(field.name, feature_class)
                if field.name == "Shape_Length":
                    arcpy.Sort_management(in_dataset=feature_class,
                                          out_dataset=os.path.join(env.workspace, "final__" + feature_class),
                                          sort_field=[["Shape_Length", "ASCENDING"]])
                if field.name == "Shape_Area":
                    arcpy.Sort_management(in_dataset=feature_class,
                                          out_dataset=os.path.join(env.workspace, "final__" + feature_class),
                                          sort_field=[["Shape_Area", "ASCENDING"]])


    def removeOldFCS(self):
        # Search for all the elements in the gdb
        # Select to delete the: - files with data, files named "new__", filese named "temp__"
        # Rename them with the original nale and remove the old ones
        env.workspace = self.path

        print "Renaming final features with the initial name. Please wait..."
        for fc in arcpy.ListFeatureClasses():
            if fc.startswith("final__"):
                arcpy.Rename_management(in_data=fc,
                                        out_data=fc + "__sorted")

        for fc in arcpy.ListFeatureClasses():
            if fc.startswith("new__"):
                arcpy.Delete_management(fc)
            if fc.startswith("temp__"):
                arcpy.Delete_management(fc)
            if fc.startswith("final__new__"):
                arcpy.Rename_management(in_data=fc,
                                        out_data=fc[12:])

        print "Script done!"

    def main(self):
        inGDB = self.inputGDB()
        bckGDB = self.backupGDB(inGDB)
        tmpFiles = self.tempFiles()
        updateTMP = self.updateTMP_Fields()
        sortFCS = self.sort_FCS(Sort.GDB_FILES)
        removeTMP = self.removeOldFCS()

if __name__ == "__main__":
    sortGDB = Sort(raw_input("Add path of the geodatabase:"))
    sortGDB.main()
    end = time.time()
    total = end - start
    print "Total time elapsed: {} seconds".format(round(total,2))



