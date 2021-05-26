from Project_ErsinAksarClasses import *

if __name__ == "__main__":
    appName = "WorldHappinessReport"
    # create object from Project_FeyzaOzkefeClasses class
    wh = WorldHappinessReport(appName)
    # take spark from Project_FeyzaOzkefeClasses  as spark
    spark = wh.spark
    wh.readAndPreprocessingDatasets()

    # Prepare spark context
    sc = spark.sparkContext

    wh.selectReportNo()
    reportNo = eval(input("Select report No: "))
    while reportNo != 0:
        wh.showHappinessReports(reportNo)
        reportNo = eval(input("Select report No: "))
